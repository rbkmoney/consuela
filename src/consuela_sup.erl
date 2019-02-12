%%%
%%% Top level supervisor

-module(consuela_sup).

%% API

-export([start_link/0]).

%%

-spec start_link() ->
    {ok, pid()} | {error, _Reason}.

start_link() ->
    Opts = maps:from_list(application:get_all_env(consuela)),
    consuela_registry_sup:start_link(Opts).
