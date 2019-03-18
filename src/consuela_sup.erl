%%%
%%% Top level supervisor

-module(consuela_sup).

%% API

-export([start_link/0]).

%%

-spec start_link() ->
    {ok, pid()} | {error, _Reason}.

start_link() ->
    genlib_adhoc_supervisor:start_link(
        #{strategy => one_for_one, intensity => 0, period => 1},
        lists:append([
            mk_discovery_childspec(genlib_app:env(consuela, discovery)),
            mk_registry_childspec(genlib_app:env(consuela, registry))
        ])
    ).

-spec mk_discovery_childspec(consuela_discovery_sup:opts() | undefined) ->
    [supervisor:child_spec()].

mk_discovery_childspec(Opts = #{}) ->
    [#{
        id    => discovery,
        start => {consuela_discovery_sup, start_link, [discovery, Opts]},
        type  => supervisor
    }];
mk_discovery_childspec(undefined) ->
    [].

-spec mk_registry_childspec(consuela_registry_sup:opts() | undefined) ->
    [supervisor:child_spec()].

mk_registry_childspec(Opts = #{}) ->
    [#{
        id    => registry,
        % Registering as a singleton on the node as it's the only way for a process to work like a process
        % registry from the point of view of OTP.
        start => {consuela_registry_sup, start_link, [consuela, Opts]},
        type  => supervisor
    }];
mk_registry_childspec(undefined) ->
    [].
