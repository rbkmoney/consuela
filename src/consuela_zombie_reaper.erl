%%%
%%% A process to defer reaping up zombie names, which process is dead but name is still possibly there in
%%% Consul.

-module(consuela_zombie_reaper).

%% api

-type registry() :: consuela_registry:t().
-type name()     :: consuela_registry:name().

-export([start_link/2]).

-export([enqueue/2]).
% TODO
% -export([dequeue/3]) ?.

-export([poke/1]).

%% gen server

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% pulse

-type beat() ::
    {{zombie, zombie()},
        enqueued |
        {reaping, succeeded | {failed, _Reason}}
    } |
    {{timer, reference()},
        {started, timeout()} |
        fired |
        reset
    } |
    {unexpected,
        {{call, from()} | cast | info, _Msg}
    }.

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

-type ref() :: pid().

-type opts() :: #{
    retry => genlib_retry:strategy(),
    pulse => {module(), _PulseOpts}
}.

-export_type([opts/0]).
-export_type([ref/0]).

-spec start_link(registry(), opts()) ->
    {ok, pid()}.

start_link(Registry, Opts) ->
    gen_server:start_link(?MODULE, {Registry, Opts}, []).


-spec enqueue(ref(), zombie()) ->
    ok.

enqueue(Ref, Zombie) ->
    gen_server:cast(Ref, {enqueue, Zombie}).

-spec poke(ref()) ->
    ok.

poke(Ref) ->
    gen_server:cast(Ref, poke).

%%

-type zombie() :: consuela_registry:reg().

-type st() :: #{
    zombies        := #{name() => _},
    queue          := queue:queue(zombie()),
    registry       := consuela_registry:t(),
    retry_strategy := genlib_retry:strategy(),
    retry_state    => genlib_retry:strategy(),
    timer          => reference(),
    pulse          := {module(), _PulseOpts}
}.

-type from() :: {pid(), reference()}.

-spec init({consuela_registry:t(), opts()}) ->
    {ok, st()}.

init({Registry, Opts}) ->
    St = maps:fold(
        fun
            (retry, V, St) ->
                _ = genlib_retry:next_step(V), % just to validate that V is indeed a strategy
                St#{retry_strategy => V};
            (pulse, {Module, _} = V, St) when is_atom(Module) ->
                St#{pulse => V}
        end,
        #{
            zombies        => #{},
            queue          => queue:new(),
            registry       => Registry,
            retry_strategy => genlib_retry:linear({max_total_timeout, 10 * 60 * 1000}, 5000),
            pulse          => {?MODULE, []}
        },
        Opts
    ),
    {ok, reset_retry_state(St)}.

-spec handle_call(_Call, from(), st()) ->
    {noreply, st()}.

handle_call(Call, From, St) ->
    _ = beat({unexpected, {{call, From}, Call}}, St),
    {noreply, St}.

-type cast() :: {enqueue, zombie()} | poke.

-spec handle_cast(cast(), st()) ->
    {noreply, st()}.

handle_cast({enqueue, Zombie}, St) ->
    {noreply, handle_enqueue(Zombie, St)};
handle_cast(poke, St) ->
    {noreply, try_force_timer(St)};
handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St}.

-type info() :: {timeout, reference(), clean}.

-spec handle_info(info(), st()) ->
    {noreply, st()}.

handle_info({timeout, TimerRef, clean}, St = #{timer := TimerRef}) ->
    _ = beat({{timer, TimerRef}, fired}, St),
    {noreply, try_clean_queue(St)};
handle_info(Info, St) ->
    _ = beat({unexpected, {info, Info}}, St),
    {noreply, St}.

-spec terminate(_Reason, st()) ->
    ok.

terminate(_Reason, _St) ->
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.

%%

handle_enqueue(Zombie = {_, Name, _}, St0 = #{zombies := Zs, queue := Q}) when not is_map_key(Name, Zs) ->
    St1 = St0#{
        zombies := mark_zombie(Zombie, Zs),
        queue   := queue:in(Zombie, Q)
    },
    _ = beat({{zombie, Zombie}, enqueued}, St1),
    % I guess that if we received a request to enqueue a zombie then Consul is unavailable, so it's better to
    % reset the timer for later.
    try_restart_timer(St1).

try_clean_queue(St0 = #{zombies := Zs, queue := Q0, registry := Registry}) ->
    {{value, Zombie}, Q1} = queue:out(Q0),
    case consuela_registry:try_unregister(Zombie, Registry) of
        {done, ok} ->
            St1 = St0#{queue := Q1, zombies := unmark_zombie(Zombie, Zs)},
            _ = beat({{zombie, Zombie}, {reaping, succeeded}}, St1),
            try_force_timer(reset_retry_state(St1));
        {failed, Reason} ->
            _ = beat({{zombie, Zombie}, {reaping, {failed, Reason}}}, St0),
            try_restart_timer(advance_retry_state(St0))
    end.

mark_zombie({_Rid, Name, _Pid}, Zs) ->
    Zs#{Name => []}.

unmark_zombie({_Rid, Name, _Pid}, Zs) ->
    maps:remove(Name, Zs).

try_restart_timer(St = #{retry_state := RetrySt}) ->
    {wait, Timeout, _RetrySt} = genlib_retry:next_step(RetrySt),
    try_restart_timer(Timeout, St).

try_force_timer(St) ->
    try_restart_timer(0, reset_retry_state(St)).

try_restart_timer(Timeout, St = #{queue := Queue}) ->
    case queue:is_empty(Queue) of
        false ->
            start_timer(Timeout, try_reset_timer(St));
        true ->
            St
    end.

try_reset_timer(St = #{timer := TimerRef}) when is_reference(TimerRef) ->
    ok = consuela_timer:reset(TimerRef),
    _ = beat({{timer, TimerRef}, reset}, St),
    maps:remove(timer, St);
try_reset_timer(St = #{}) ->
    St.

start_timer(Timeout, St = #{}) when not is_map_key(timer, St) ->
    TimerRef = consuela_timer:start(Timeout, clean),
    _ = beat({{timer, TimerRef}, {started, Timeout}}, St),
    St#{timer => TimerRef}.

reset_retry_state(St = #{retry_strategy := Retry}) ->
    % TODO
    % Will not work well with timecapped strategies, we need to separate constructor from state.
    St#{retry_state => Retry}.

advance_retry_state(St = #{retry_state := RetrySt0}) ->
    case genlib_retry:next_step(RetrySt0) of
        {wait, _Timeout, RetrySt1} ->
            % We already slept that timeout earlier
            St#{retry_state := RetrySt1};
        finish ->
            % No reason to live anymore
            exit(retries_exhausted)
    end.

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
