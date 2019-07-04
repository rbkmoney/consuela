%%%
%%% Leader supervisor
%%%
%%% NOTES
%%%
%%% A simple supervisor behaviour wrapper designed to implement _only one in a cluster_ guarantee. Some node
%%% who is quick enough to register a name will run the supervisor code while those not so lucky will run
%%% special warden logic which only purpose is to look for liveness of the lucky supervisor and, if it looks
%%% dead, try to take over the name.
%%%

-module(consuela_leader_supervisor).

%%

-type name() :: consuela_registry:name().
-type modargs(Args) :: {module(), Args}.

-export([start_link/4]).
-export([which_children/1]).

-type opts() :: #{
    % For better performance and stability this strategy should be aligned with active session's lock delay,
    % in the sense that strategy's max total timeout should be more than that but not much more.
    retry => genlib_retry:strategy(),
    pulse => {module(), _PulseOpts}
}.

-export_type([opts/0]).

%% gen server lookalike

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%%

-type beat() ::
    {{warden, name()}, {started | stopped, pid()}} |
    {{leader, name()}, {started, pid()} | {down, pid() | undefined, _Reason}} |
    {{timer, reference()}, {started, _Msg, timeout()} | fired} |
    {{monitor, reference()}, set | fired} |
    {unexpected, {{call, from()} | cast | info, _Msg}}.

-export_type([beat/0]).

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

-spec start_link(name(), module(), _Args, opts()) ->
    {ok, pid()} | {error, Reason} when
        Reason :: {shutdown, term()} | term().

start_link(Name, Module, Args, Opts) ->
    gen_server:start_link(?MODULE, {Name, {Module, Args}, Opts}, []).

-spec which_children(name()) ->
    [{_ChildID | undefined, pid() | restarting | undefined, worker | supervisor, [module()] | dynamic}].

which_children(Name) ->
    supervisor:which_children(mk_reg_name(Name)).

%%

-type from() :: {pid(), reference()}.

-type st() :: #{
    name           := name(),
    modargs        := modargs(_),
    state          := {handoff, _SupervisorSt} | takeover | {monitor, pid(), reference()},
    retry_strategy := genlib_retry:strategy(),
    retry_state    => genlib_retry:strategy(),
    tref           => reference(),
    pulse          := {module(), _PulseOpts}
}.

-spec init({name(), modargs(_), opts()}) ->
    {ok, _SupervisorSt | st()} |
    {ok, _SupervisorSt | st(), timeout() | hibernate} |
    ignore.

init({Name, ModArgs, Opts}) ->
    St0 = mk_state(Name, ModArgs, Opts),
    _ = beat({{warden, Name}, {started, self()}}, St0),
    case handle_takeover(reset_retry_state(St0)) of
        {ok, St1} ->
            {ok, St1, hibernate};
        {{error, _} = Error, _St} ->
            {stop, Error}
    end.

-spec mk_state(name(), modargs(_), opts()) ->
    st().

mk_state(Name, ModArgs, Opts) ->
    #{
        name           => Name,
        modargs        => ModArgs,
        state          => takeover,
        pulse          => maps:get(pulse, Opts, {?MODULE, []}),
        retry_strategy => maps:get(retry, Opts, genlib_retry:linear({max_total_timeout, 30 * 1000}, 5000))
    }.

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
    {timeout, reference(), takeover | handoff} |
    {'DOWN', reference(), process, pid(), _Reason}.

-spec handle_info(info(), st()) ->
    {noreply, st(), hibernate}.

handle_info({timeout, TRef, Msg}, St) ->
    _ = beat({{timer, TRef}, fired}, St),
    noreply(handle_timeout(TRef, Msg, St));
handle_info({'DOWN', MRef, process, Pid, Reason}, St) ->
    _ = beat({{monitor, MRef}, fired}, St),
    noreply(handle_leader_down(MRef, Pid, Reason, St));
handle_info(Msg, St) ->
    _ = beat({unexpected, {info, Msg}}, St),
    {noreply, St, hibernate}.

-spec noreply
    ({ok, st()}) ->
        {noreply, st(), hibernate};
    ({{error, Reason}, st()}) ->
        {stop, {error, Reason}, st()}.

noreply({ok, St}) ->
    {noreply, St, hibernate};
noreply({{error, _} = Error, St}) ->
    {stop, Error, St}.

handle_timeout(TRef, Msg, St = #{tref := TRef}) ->
    handle_timeout(Msg, maps:remove(tref, St)).

handle_timeout(takeover, St) ->
    handle_takeover(St);
handle_timeout(handoff, St) ->
    handle_handoff(St).

handle_takeover(St = #{state := takeover, name := Name}) ->
    try
        case consuela:register_name(Name, self()) of
            yes ->
                % Woo! We need to hand control over to supervisor right away.
                handoff(St);
            no ->
                case consuela:whereis_name(Name) of
                    Pid when is_pid(Pid), node(Pid) /= node() ->
                        {ok, start_monitor(Pid, reset_retry_state(St))};
                    _ ->
                        % Smells like _lock-delay_ scenario.
                        defer_takeover(St)
                end
        end
    catch
        exit:{consuela, _} ->
            % Consul is unstable
            defer_takeover(St)
    end.

handle_handoff(#{state := {handoff, SupervisorSt}, name := Name}) ->
    gen_server:enter_loop(supervisor, [], SupervisorSt, mk_reg_name(Name)).

handle_leader_down(MRef, Pid, Reason, St0 = #{state := {monitor, Pid, MRef}, name := Name}) ->
    _ = beat({{leader, Name}, {down, Pid, Reason}}, St0),
    St1 = St0#{state := takeover},
    case Reason of
        % Precaution against tight monitor-takeover loops
        noconnection -> defer_takeover(St1);
        noproc       -> defer_takeover(St1);
        _            -> handle_takeover(St1)
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

start_monitor(Pid, St0 = #{state := takeover}) ->
    MRef = erlang:monitor(process, Pid),
    St1 = St0#{state => {monitor, Pid, MRef}},
    _ = beat({{monitor, MRef}, set}, St1),
    St1.

handoff(St = #{state := takeover, name := Name, modargs := {Module, Args}}) ->
    % TODO less confusing beats
    _ = beat({{warden, Name}, {stopped, self()}}, St),
    RegName = mk_reg_name(Name),
    case supervisor:init({RegName, Module, Args}) of
        {ok, SupervisorSt} ->
            _ = beat({{leader, Name}, {started, self()}}, St),
            defer_handoff(St#{state => {handoff, SupervisorSt}});
        {stop, Reason} ->
            {{error, Reason}, St}
    end.

defer_takeover(St) ->
    defer(takeover, St).

defer_handoff(St) ->
    defer(handoff, 0, St).

defer(Message, St = #{name := Name, retry_state := RetrySt0}) ->
    false = maps:is_key(tref, St),
    case genlib_retry:next_step(RetrySt0) of
        {wait, Timeout, RetrySt1} ->
            defer(Message, Timeout, St#{retry_state := RetrySt1});
        finish ->
            _ = beat({{warden, Name}, {stopped, self()}}, St),
            {{error, retries_exhausted}, St}
    end.

defer(Message, Timeout, St0) ->
    false = maps:is_key(tref, St0),
    TRef = consuela_timer:start(Timeout, Message),
    St1 = St0#{tref => TRef},
    _ = beat({{timer, TRef}, {started, Message, Timeout}}, St1),
    {ok, St1}.

reset_retry_state(St0 = #{retry_strategy := Retry}) ->
    St0#{retry_state => Retry}.

%%

-spec mk_reg_name(name()) ->
    {via, atom(), name()}.

mk_reg_name(Name) ->
    {via, consuela, Name}.

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
