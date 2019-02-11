%%%
%%% OTP Registry interface

-module(consuela).

%%

-type name() :: consuela_registry:name().

-export([register_name/2]).
-export([unregister_name/1]).
-export([whereis_name/1]).

%%

-spec register_name(name(), pid()) ->
    yes | no. % Lol why

-spec unregister_name(name()) ->
    _.

-spec whereis_name(name()) ->
    pid() | undefined.

register_name(Name, Pid) ->
    case consuela_registry:register(Name, Pid) of
        ok ->
            yes;
        {error, exists} ->
            no
    end.

unregister_name(Name) ->
    ok = consuela_registry:unregister(Name, self()),
    ok.

whereis_name(Name) ->
    case consuela_registry:lookup(Name) of
        {ok, Pid} ->
            Pid;
        {error, notfound} ->
            undefined
    end.
