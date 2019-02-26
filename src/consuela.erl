%%%
%%% OTP Registry interface

-module(consuela).

%%

-type name() :: consuela_registry:name().

-export([all/0]).

-export([register_name/2]).
-export([unregister_name/1]).
-export([whereis_name/1]).

%%

-spec all() ->
    [{name(), pid()}].

all() ->
    consuela_registry_server:all(consuela).

%%

-spec register_name(name(), pid()) ->
    yes | no. % Lol why

-spec unregister_name(name()) ->
    _.

-spec whereis_name(name()) ->
    pid() | undefined.

register_name(Name, Pid) ->
    case consuela_registry_server:register(consuela, Name, Pid) of
        ok ->
            yes;
        {error, exists} ->
            no
    end.

unregister_name(Name) ->
    ok = consuela_registry_server:unregister(consuela, Name, self()),
    ok.

whereis_name(Name) ->
    case consuela_registry_server:lookup(consuela, Name) of
        {ok, Pid} ->
            Pid;
        {error, notfound} ->
            undefined
    end.
