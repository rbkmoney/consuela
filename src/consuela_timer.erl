%%%
%%% Timer management utilities

-module(consuela_timer).

%% api

-export([start/2]).
-export([reset/1]).

%%

-type timer_ref() :: reference().

-spec start(_Timeout :: non_neg_integer(), _Msg) -> timer_ref().
start(Timeout, Msg) ->
    erlang:start_timer(Timeout, self(), Msg).

-spec reset(timer_ref()) -> ok.
reset(TimerRef) ->
    case erlang:cancel_timer(TimerRef) of
        false ->
            ok = flush(TimerRef);
        _Time ->
            ok
    end.

flush(TimerRef) ->
    receive
        {timeout, TimerRef, _} ->
            ok
    after 0 -> ok
    end.
