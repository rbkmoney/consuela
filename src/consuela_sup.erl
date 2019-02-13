%%%
%%% Top level supervisor

-module(consuela_sup).

%% API

-export([start_link/0]).

%%

-spec start_link() ->
    {ok, pid()} | {error, _Reason}.

start_link() ->
    % Registering as a singleton on the node as it's the only way for a process to work like a process
    % registry from the point of view of OTP.
    Opts = maps:from_list(application:get_all_env(consuela)),
    consuela_registry_sup:start_link(consuela, Opts).
