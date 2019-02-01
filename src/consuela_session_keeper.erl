%%%
%%% Consul session keeper

-module(consuela_session_keeper).

%% api

-type interval() ::
    pos_integer()       | % milliseconds
    genlib_rational:t() . % fraction of TTL

-export([start_link/3]).

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
    {session              , {renewed | destroyed, consuela_session:t()}} |
    {{timer, reference()} , {started, timeout()} | fired | reset} |
    {unexpected           , {{call, from()} | cast | info, _Msg}}.

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

-type opts() :: #{
    interval => interval(),
    pulse    => {module(), _PulseOpts}
}.

-spec start_link(consuela_session:t(), consuela_client:t(), opts()) ->
    {ok, pid()}.

start_link(Session, Client, Opts) ->
    gen_server:start_link(?MODULE, {Session, Client, Opts}, []).

%%

-type st() :: #{
    session  => consuela_session:t(),
    client   := consuela_client:t(),
    interval := interval(),
    timer    => reference(),
    pulse    := {module(), _PulseOpts}
}.

-type from() :: {pid(), reference()}.

-spec init({consuela_session:t(), consuela_client:t(), opts()}) ->
    {ok, st()}.

init({Session, Client, Opts}) ->
    St = maps:fold(
        fun
            (pulse, {Module, _} = V, St) when is_atom(Module) ->
                St#{pulse => V};
            (interval, {P, Q}, St) when is_integer(P), is_integer(Q), Q > 0, P > 0 ->
                St#{interval => genlib_rational:new(P, Q)};
            (interval, Timeout, St) when is_integer(Timeout), Timeout > 0 ->
                St#{interval => Timeout}
        end,
        #{
            session  => Session,
            client   => Client,
            interval => {1, 3}, % third of a TTL by default
            pulse    => {?MODULE, []}
        },
        Opts
    ),
    {ok, St, 0}.

-spec handle_call(_Call, from(), st()) ->
    {noreply, st()}.

handle_call(Call, From, St) ->
    _ = beat({unexpected, {{call, From}, Call}}, St),
    {noreply, St}.

-spec handle_cast(_Cast, st()) ->
    {noreply, st()}.

handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St}.

-spec handle_info(_Info, st()) ->
    {noreply, st()}.

handle_info({timeout, TimerRef, renew}, St = #{timer := TimerRef}) ->
    _ = beat({{timer, TimerRef}, fired}, St),
    {noreply, restart_timer(renew_session(St))};
handle_info(timeout, St) ->
    {noreply, restart_timer(renew_session(St))};
handle_info(Info, St) ->
    _ = beat({unexpected, {info, Info}}, St),
    {noreply, St}.

-spec terminate(_Reason, st()) ->
    _.

terminate(_Reason, St) ->
    destroy_session(St).

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.

%%

renew_session(St0 = #{session := #{id := ID}, client := Client}) ->
    {ok, Session} = consuela_session:renew(ID, Client),
    St = St0#{session := Session},
    _ = beat({session, {renewed, Session}}, St),
    St.

destroy_session(St0 = #{session := Session = #{id := ID}, client := Client}) ->
    ok = consuela_session:destroy(ID, Client),
    St = maps:remove(session, St0),
    _ = beat({session, {destroyed, Session}}, St),
    St.

%%

restart_timer(St) ->
    start_timer(try_reset_timer(St)).

start_timer(St0) ->
    Timeout = compute_timeout(St0),
    TimerRef = erlang:start_timer(Timeout, self(), renew),
    St = St0#{timer => TimerRef},
    _ = beat({{timer, TimerRef}, {started, Timeout}}, St),
    St.

compute_timeout(#{interval := Ratio, session := #{ttl := TTL}}) when is_tuple(Ratio) ->
    genlib_rational:round(genlib_rational:mul(Ratio, genlib_rational:new(TTL * 1000)));
compute_timeout(#{interval := Timeout}) when is_integer(Timeout) ->
    Timeout.

try_reset_timer(St0 = #{timer := TimerRef}) ->
    St = case erlang:cancel_timer(TimerRef) of
        false ->
            ok = flush_timer(TimerRef),
            maps:remove(timer, St0);
        _Time ->
            St0
    end,
    _ = beat({{timer, TimerRef}, reset}, St),
    St;
try_reset_timer(St) ->
    St.

flush_timer(TimerRef) ->
    receive {timeout, TimerRef, _} ->
        ok
    after 0 ->
        ok
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
