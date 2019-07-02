%%%
%%% Leader warden

-module(consuela_leader_warden).

%%

-type name() :: consuela_registry:name().

-type opts() :: #{
    % For better performance and stability this strategy should be aligned with active session's lock delay,
    % in the sense that strategy's max total timeout should be more than that but not much more.
    retry => genlib_retry:strategy(),
    pulse => {module(), _PulseOpts}
}.

-export_type([opts/0]).

-export([start_link/3]).

%% gen server

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%%

-type beat() ::
    {{warden, name()}, {started | stopped, pid()}} |
    {{leader, name()}, {down, pid() | undefined, _Reason}} |
    {{timer, reference()}, {started, _Msg, timeout()} | fired} |
    {{monitor, reference()}, set | fired} |
    {unexpected, {{call, from()} | cast | info, _Msg}}.

-export_type([beat/0]).

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

-spec start_link(name(), pid() | undefined, opts()) ->
    {ok, pid()}.

start_link(Name, LeaderPid, Opts) ->
    St = mk_state(Name, LeaderPid, Opts),
    {ok, Pid} = gen_server:start_link(?MODULE, St, []),
    _ = beat({{warden, Name}, {started, Pid}}, St),
    {ok, Pid}.

%%

-type st() :: #{
    name           := name(),
    pid            := pid() | undefined,
    mref           => reference(),
    retry_strategy := genlib_retry:strategy(),
    retry_state    => genlib_retry:strategy(),
    tref           => reference(),
    pulse          := {module(), _PulseOpts}
}.

-type from() :: {pid(), reference()}.

-spec mk_state(name(), pid() | undefined, opts()) ->
    st().

mk_state(Name, Pid, Opts) ->
    #{
        name           => Name,
        pid            => Pid,
        pulse          => maps:get(pulse, Opts, {?MODULE, []}),
        retry_strategy => maps:get(retry, Opts, genlib_retry:linear({max_total_timeout, 30 * 1000}, 5000))
    }.

-spec init(st()) ->
    {ok, st(), hibernate}.

init(St) ->
    {ok, remonitor(reset_retry_state(St)), hibernate}.

-spec handle_call(_Call, from(), st()) ->
    {noreply, st(), hibernate}.

handle_call(Call, From, St) ->
    _ = beat({unexpected, {{call, From}, Call}}, St),
    {noreply, St, hibernate}.

-spec handle_cast(_Cast, st()) ->
    {noreply, st(), hibernate}.

handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St, hibernate}.

-type info() ::
    {timeout, reference(), remonitor} |
    {'DOWN', reference(), process, pid(), _Reason} |
    'DOWN'.

-spec handle_info(info(), st()) ->
    {stop, _Reason, st()} | {noreply, st(), hibernate}.

handle_info({'DOWN', MRef, process, Pid, Reason}, St) ->
    _ = beat({{monitor, MRef}, fired}, St),
    handle_down(MRef, Pid, Reason, St);
handle_info('DOWN', St) ->
    handle_down(undefined, noproc, St);
handle_info({timeout, TRef, Message}, St) ->
    _ = beat({{timer, TRef}, fired}, St),
    handle_timeout(TRef, Message, St);
handle_info(Info, St) ->
    _ = beat({unexpected, {info, Info}}, St),
    {noreply, St, hibernate}.

-spec handle_down(reference(), pid() | undefined, _Reason, st()) ->
    {stop, _Reason, st()} | {noreply, st()}.

handle_down(MRef, Pid, Reason, St = #{mref := MRef}) ->
    handle_down(Pid, Reason, maps:remove(mref, St)).

handle_down(Pid, Reason, St = #{name := Name, pid := Pid}) ->
    _ = beat({{leader, Name}, {down, Pid, Reason}}, St),
    handle_down(Reason, St).

handle_down(noconnection, St) ->
    handle_node_down(St);
handle_down(Reason, St) ->
    handle_process_down(Reason, St).

handle_node_down(St = #{retry_state := Retry}) ->
    case genlib_retry:next_step(Retry) of
        {wait, Timeout, Retry1} ->
            {noreply, defer_remonitor(Timeout, St#{retry_state := Retry1}), hibernate};
        finish ->
            handle_process_down(noconnection, St)
    end.

handle_process_down(Reason, St = #{name := Name, pid := Pid}) when is_pid(Pid) ->
    try consuela:whereis_name(Name) of
        Pid ->
            handle_process_ghost(Reason, St);
        _ ->
            go_down(Reason, St)
    catch
        exit:{consuela, {_Class, _Reason}} ->
            handle_process_ghost(Reason, St)
    end;
handle_process_down(Reason, St = #{pid := undefined}) ->
    handle_process_ghost(Reason, St).

handle_process_ghost(Reason, St = #{retry_state := Retry}) ->
    case genlib_retry:next_step(Retry) of
        {wait, Timeout, Retry1} ->
            {noreply, defer_shutdown(Reason, Timeout, St#{retry_state := Retry1}), hibernate};
        finish ->
            go_down(Reason, St)
    end.

go_down(Reason, St = #{name := Name}) ->
    _ = beat({{warden, Name}, {stopped, self()}}, St),
    {stop, mk_stop_reason(Reason), St}.

mk_stop_reason(Reason) when Reason == normal; Reason == shutdown; element(1, Reason) == shutdown ->
    {shutdown, {?MODULE, {leader_lost, Reason}}};
mk_stop_reason(Reason) ->
    {error, {?MODULE, {leader_lost, Reason}}}.

-spec handle_timeout(reference(), remonitor, st()) ->
    {noreply, st(), hibernate}.

handle_timeout(TRef, remonitor, St = #{tref := TRef}) ->
    handle_remonitor(maps:remove(tref, St));
handle_timeout(TRef, {shutdown, Reason}, St = #{tref := TRef}) ->
    handle_process_down(Reason, maps:remove(tref, St)).

handle_remonitor(St = #{pid := Pid}) ->
    case net_kernel:connect_node(node(Pid)) of
        true ->
            {noreply, remonitor(reset_retry_state(St)), hibernate};
        false ->
            handle_node_down(St)
    end.

-spec terminate(_Reason, st()) ->
    ok.

terminate(_Reason, _St) ->
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.

%%

remonitor(St0 = #{pid := Pid}) when is_pid(Pid) ->
    false = maps:is_key(mref, St0),
    MRef = erlang:monitor(process, Pid),
    St1 = St0#{mref => MRef},
    _ = beat({{monitor, MRef}, set}, St1),
    St1;
remonitor(St0 = #{pid := undefined}) ->
    _ = self() ! 'DOWN',
    St0.

defer_remonitor(Timeout, St) ->
    defer(remonitor, Timeout, St).

defer_shutdown(Reason, Timeout, St) ->
    defer({shutdown, Reason}, Timeout, St).

defer(Message, Timeout, St0 = #{}) ->
    false = maps:is_key(tref, St0),
    TRef = consuela_timer:start(Timeout, Message),
    St1 = St0#{tref => TRef},
    _ = beat({{timer, TRef}, {started, Message, Timeout}}, St1),
    St1.

reset_retry_state(St0 = #{retry_strategy := Retry}) ->
    St0#{retry_state => Retry}.

%%

-spec beat(beat(), st()) ->
    _.

beat(Beat, #{pulse := {Module, PulseOpts}}) ->
    % TODO handle errors?
    Module:handle_beat(Beat, PulseOpts).

-spec handle_beat(beat(), [trace]) ->
    ok.

handle_beat(Beat, [trace]) ->
    logger:debug("[~p] ~p", [?MODULE, Beat]);
handle_beat(_Beat, []) ->
    ok.
