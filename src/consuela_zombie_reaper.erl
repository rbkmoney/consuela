%%%
%%% A process to defer reaping up zombie names, which process is dead but name is still possibly there in
%%% Consul. Also can be used to asynchronously reap registrations when a processes die for example.

-module(consuela_zombie_reaper).

%% api

-type registry() :: consuela_registry:t().

-export([start_link/2]).

-export([enqueue/2]).
-export([enqueue/3]).
% TODO
% -export([dequeue/3]) ?.

-export([drain/1]).

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
        {reaping, succeeded | {skipped, _Reason} | {failed, _Reason}}
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

-export_type([beat/0]).

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
    enqueue(Ref, Zombie, #{}).

-spec enqueue(ref(), zombie(), #{drain => boolean()}) ->
    ok.

enqueue(Ref, Zombie, Opts) ->
    gen_server:cast(Ref, {enqueue, Zombie, Opts}).

-spec drain(ref()) ->
    ok.

drain(Ref) ->
    gen_server:cast(Ref, drain).

%%

-type zombie() :: consuela_registry:reg().

-type st() :: #{
    queue          := queue:queue(zombie()),
    registry       := consuela_registry:t(),
    retry_strategy := genlib_retry:strategy(),
    retry_state    => genlib_retry:strategy(),
    timeout        => timeout(),
    timer          => reference(),
    pulse          := {module(), _PulseOpts}
}.

-type from() :: {pid(), reference()}.

-spec init({consuela_registry:t(), opts()}) ->
    {ok, st()}.

init({Registry, Opts}) ->
    % to have a chance to drain queue containing processes which just died
    _ = erlang:process_flag(trap_exit, true),
    St = maps:fold(
        fun
            (retry, V, St) ->
                St#{retry_strategy => V};
            (pulse, {Module, _} = V, St) when is_atom(Module) ->
                St#{pulse => V}
        end,
        #{
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

-type cast() :: {enqueue, zombie()} | drain.

-spec handle_cast(cast(), st()) ->
    {noreply, st()}.

handle_cast({enqueue, Zombie, Opts}, St) ->
    {noreply, handle_enqueue(Zombie, Opts, St)};
handle_cast(drain, St) ->
    {noreply, try_drain_queue(St)};
handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St}.

-type info() :: {timeout, reference(), clean}.

-spec handle_info(info(), st()) ->
    {noreply, st()}.

handle_info({timeout, TimerRef, clean}, St = #{timer := TimerRef}) ->
    _ = beat({{timer, TimerRef}, fired}, St),
    {noreply, try_clean_queue(regular, maps:remove(timer, St))};
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

handle_enqueue(Zombie, Opts, St0 = #{queue := Q}) ->
    St1 = St0#{queue := queue:in(Zombie, Q)},
    _ = beat({{zombie, Zombie}, enqueued}, St1),
    case Opts of
        #{drain := true} ->
            try_clean_queue(draining, St1);
        #{} ->
            try_start_timer(St1)
    end.

try_drain_queue(St = #{queue := Queue}) ->
    case queue:is_empty(Queue) of
        false ->
            try_clean_queue(draining, St);
        true ->
            St
    end.

try_clean_queue(Mode, St0 = #{queue := Q0, registry := Registry}) ->
    {{value, Zombie}, Q1} = queue:out(Q0),
    case consuela_registry:try_unregister(Zombie, Registry) of
        {done, ok} ->
            St1 = St0#{queue := Q1},
            _ = beat({{zombie, Zombie}, {reaping, succeeded}}, St1),
            try_force_timer(reset_retry_state(St1));
        {done, {error, stale}} ->
            St1 = St0#{queue := Q1},
            _ = beat({{zombie, Zombie}, {reaping, {skipped, stale}}}, St1),
            try_force_timer(reset_retry_state(St1));
        {failed, Reason} ->
            _ = beat({{zombie, Zombie}, {reaping, {failed, Reason}}}, St0),
            case Mode of
                regular ->
                    start_timer(advance_retry_state(St0));
                draining ->
                    try_start_timer(St0)
            end
    end.

try_force_timer(St) ->
    try_start_timer(0, try_reset_timer(St)).

try_start_timer(St = #{timeout := Timeout}) ->
    try_start_timer(Timeout, St).

try_start_timer(_Timeout, St = #{timer := _}) ->
    St;
try_start_timer(Timeout, St = #{queue := Queue}) ->
    case queue:is_empty(Queue) of
        false ->
            start_timer(Timeout, St);
        true ->
            St
    end.

try_reset_timer(St = #{timer := TimerRef}) when is_reference(TimerRef) ->
    ok = consuela_timer:reset(TimerRef),
    _ = beat({{timer, TimerRef}, reset}, St),
    maps:remove(timer, St);
try_reset_timer(St = #{}) ->
    St.

start_timer(St = #{timeout := Timeout}) ->
    start_timer(Timeout, St).

start_timer(Timeout, St = #{}) ->
    false = maps:is_key(timer, St),
    TimerRef = consuela_timer:start(Timeout, clean),
    _ = beat({{timer, TimerRef}, {started, Timeout}}, St),
    St#{timer => TimerRef}.

reset_retry_state(St = #{retry_strategy := Retry}) ->
    % TODO
    % Will not work well with timecapped strategies, we need to separate constructor from state.
    advance_retry_state(St#{retry_state => Retry}).

advance_retry_state(St = #{retry_state := RetrySt0}) ->
    case genlib_retry:next_step(RetrySt0) of
        {wait, Timeout, RetrySt1} ->
            St#{retry_state := RetrySt1, timeout => Timeout};
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
