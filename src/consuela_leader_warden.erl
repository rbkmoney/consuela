%%%
%%% Leader warden

-module(consuela_leader_warden).

%%

-type name() :: consuela_registry:name().

-type opts() :: #{
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
    {{leader, name()}, {down, pid(), _Reason}} |
    {{timer, reference()}, {started, timeout()} | fired} |
    {{monitor, reference()}, set | fired} |
    {unexpected, {{call, from()} | cast | info, _Msg}}.

-export_type([beat/0]).

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

-spec start_link(name(), pid(), opts()) ->
    {ok, pid()}.

start_link(Name, Pid, Opts) ->
    St = mk_state(Name, Pid, Opts),
    {ok, Pid} = gen_server:start_link(?MODULE, St, []),
    _ = beat({{warden, Name}, {started, Pid}}, St),
    {ok, Pid}.

%%

-define(DEFER_TIMEOUT, 1000).

-type st() :: #{
    name  := name(),
    pid   := pid(),
    mref  => reference(),
    tref  => reference(),
    pulse := {module(), _PulseOpts}
}.

-type from() :: {pid(), reference()}.

-spec mk_state(name(), pid(), opts()) ->
    st().

mk_state(Name, Pid, Opts) ->
    #{
        name  => Name,
        pid   => Pid,
        pulse => maps:get(pulse, Opts, {?MODULE, []})
    }.

-spec init(st()) ->
    {ok, st(), hibernate}.

init(St) ->
    {ok, remonitor(St), hibernate}.

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
    {'DOWN', reference(), process, pid(), _Reason}.

-spec handle_info(info(), st()) ->
    {stop, _Reason, st()} | {noreply, st(), hibernate}.

handle_info({'DOWN', MRef, process, Pid, Reason}, St) ->
    _ = beat({{monitor, MRef}, fired}, St),
    handle_down(MRef, Pid, Reason, St);
handle_info({timeout, TRef, Message}, St) ->
    _ = beat({{timer, TRef}, fired}, St),
    handle_timeout(TRef, Message, St);
handle_info(Info, St) ->
    _ = beat({unexpected, {info, Info}}, St),
    {noreply, St, hibernate}.

-spec handle_down(reference(), pid(), _Reason, st()) ->
    {stop, _Reason, st()} | {noreply, st()}.

handle_down(MRef, Pid, Reason, St = #{mref := MRef, name := Name, pid := Pid}) ->
    _ = beat({{leader, Name}, {down, Pid, Reason}}, St),
    handle_down(Reason, maps:remove(mref, St)).

handle_down(noconnection, St) ->
    {noreply, defer_remonitor(St), hibernate};
handle_down(Reason, St = #{name := Name}) ->
    _ = beat({{warden, Name}, {stopped, self()}}, St),
    {stop, {?MODULE, {leader_lost, Reason}}, St}.

-spec handle_timeout(reference(), remonitor, st()) ->
    {noreply, st(), hibernate}.

handle_timeout(TRef, remonitor, St = #{tref := TRef}) ->
    {noreply, remonitor(maps:remove(tref, St)), hibernate}.

-spec terminate(_Reason, st()) ->
    ok.

terminate(_Reason, _St) ->
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.

%%

remonitor(St0 = #{pid := Pid}) ->
    false = maps:is_key(mref, St0),
    MRef = erlang:monitor(process, Pid),
    St1 = St0#{mref => MRef},
    _ = beat({{monitor, MRef}, set}, St1),
    St1.

defer_remonitor(St0 = #{}) ->
    false = maps:is_key(tref, St0),
    Timeout = ?DEFER_TIMEOUT,
    TRef = consuela_timer:start(Timeout, remonitor),
    St1 = St0#{tref => TRef},
    _ = beat({{timer, TRef}, {started, Timeout}}, St1),
    St1.

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
