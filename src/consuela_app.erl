%%%
%%% Application startup and shutdown

-module(consuela_app).

%%

-behaviour(application).
-export([start/2]).
-export([stop/1]).

%%

-spec start(normal, _StartArgs) ->
    {ok, pid()} | {error, _Reason}.

start(_StartType, _StartArgs) ->
    consuela_sup:start_link().

-spec stop(_State) ->
    ok.

stop(_State) ->
    ok.
