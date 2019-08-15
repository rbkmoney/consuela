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

%%% TODO
%%% Strictly speaking nothing obliges us to require supervisor-like module as an underlying behaviour anymore.
%%% We need at least gen_server behaviour which is more generic. Moreover implementation would be less
%%% _hackish_ that way because we already abuse _supervisor-is-a-gen-server_ internal details and we would not
%%% need to no more.

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
-export([handle_continue/2]).
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
    context        := init | handle,
    retry_strategy := genlib_retry:strategy(),
    retry_state    => genlib_retry:strategy(),
    tref           => reference(),
    pulse          := {module(), _PulseOpts}
}.

-spec init({name(), modargs(_), opts()}) ->
    {ok, st() | _SupervisorSt} |
    {ok, st() | _SupervisorSt, timeout() | {continue, _} | hibernate} |
    {stop, _Reason} |
    ignore.

init({Name, ModArgs, Opts}) ->
    St = mk_state(Name, ModArgs, Opts),
    _ = beat({{warden, Name}, {started, self()}}, St),
    handle_takeover(reset_retry_state(St)).

-spec mk_state(name(), modargs(_), opts()) ->
    st().

mk_state(Name, ModArgs, Opts) ->
    #{
        name           => Name,
        modargs        => ModArgs,
        state          => takeover,
        context        => init,
        pulse          => maps:get(pulse, Opts, {?MODULE, []}),
        retry_strategy => maps:get(retry, Opts, genlib_retry:exponential({max_total_timeout, 30 * 1000}, 2, 1000, 5000))
    }.

-spec handle_continue(handoff, st()) ->
    no_return().

handle_continue(handoff, St) ->
    finish_handoff(St).

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
    {timeout, reference(), takeover} |
    {'DOWN', reference(), process, pid(), _Reason}.

-spec handle_info(info(), st()) ->
    {noreply, st(), hibernate} |
    {stop, _Reason, st()}.

handle_info({timeout, TRef, Msg}, St) ->
    _ = beat({{timer, TRef}, fired}, St),
    handle_timeout(TRef, Msg, St);
handle_info({'DOWN', MRef, process, Pid, Reason}, St) ->
    _ = beat({{monitor, MRef}, fired}, St),
    handle_leader_down(MRef, Pid, Reason, St);
handle_info(Msg, St) ->
    _ = beat({unexpected, {info, Msg}}, St),
    ok(St).

-spec ok(st()) ->
    {ok | noreply, st(), hibernate}.

ok(St = #{context := init}) ->
    {ok, St#{context := handle}, hibernate};
ok(St = #{context := handle}) ->
    {noreply, St, hibernate}.

-spec stop(Reason, st()) ->
    {stop, Reason} |
    {stop, Reason, st()}.

stop(Reason, _St = #{context := init}) ->
    {stop, Reason};
stop(Reason, St = #{context := handle}) ->
    {stop, Reason, St}.

handle_timeout(TRef, Msg, St = #{tref := TRef}) ->
    handle_timeout(Msg, maps:remove(tref, St)).

handle_timeout(takeover, St) ->
    handle_takeover(St).

handle_takeover(St = #{state := takeover, name := Name}) ->
    try
        case consuela:register_name(Name, self()) of
            yes ->
                % Woo! We need to hand control over to supervisor right away.
                handoff(St);
            no ->
                case consuela:whereis_name(Name) of
                    Pid when is_pid(Pid), node(Pid) /= node() ->
                        ok(start_monitor(Pid, reset_retry_state(St)));
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

handle_leader_down(MRef, Pid, Reason, St0 = #{state := {monitor, Pid, MRef}, name := Name}) ->
    _ = beat({{leader, Name}, {down, Pid, Reason}}, St0),
    St1 = St0#{state := takeover},
    % Precaution against too tight monitor-takeover loops
    defer_takeover(St1).

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

handoff(St = #{state := takeover, context := Ctx, name := Name, modargs := {Module, Args}}) ->
    % TODO less confusing beats
    _ = beat({{warden, Name}, {stopped, self()}}, St),
    RegName = mk_reg_name(Name),
    case supervisor:init({RegName, Module, Args}) of
        {ok, SupervisorSt} ->
            _ = beat({{leader, Name}, {started, self()}}, St),
            St1 = St#{state => {handoff, SupervisorSt}},
            case Ctx of
                init ->
                    {ok, St1, {continue, handoff}};
                handle ->
                    finish_handoff(St1)
            end;
        {stop, Reason} ->
            stop(Reason, St)
    end.

finish_handoff(#{state := {handoff, SupervisorSt}, name := Name}) ->
    gen_server:enter_loop(supervisor, [], SupervisorSt, mk_reg_name(Name)).

defer_takeover(St) ->
    defer(takeover, St).

defer(Message, St = #{name := Name, retry_state := RetrySt0}) ->
    false = maps:is_key(tref, St),
    case genlib_retry:next_step(RetrySt0) of
        {wait, Timeout, RetrySt1} ->
            defer(Message, Timeout, St#{retry_state := RetrySt1});
        finish ->
            _ = beat({{warden, Name}, {stopped, self()}}, St),
            stop({error, retries_exhausted}, St)
    end.

defer(Message, Timeout, St0) ->
    false = maps:is_key(tref, St0),
    TRef = consuela_timer:start(Timeout, Message),
    St1 = St0#{tref => TRef},
    _ = beat({{timer, TRef}, {started, Message, Timeout}}, St1),
    ok(St1).

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
