-module(discovery_node_runner).

-include_lib("kernel/include/inet.hrl").

-export([run/3]).

-export([handle_beat/2]).

%%

-spec run(binary(), consuela_client:url(), pos_integer()) -> ok.
run(Nodename, ConsulUrl, Lifetime) ->
    ok = logger:set_primary_config(level, info),
    _Apps = genlib_app:start_application(consuela),
    ok = ct_consul:await_ready(),
    Consul = #{
        url => ConsulUrl,
        opts => opts(consul)
    },
    PresenceOpts = #{
        name => Nodename,
        consul => Consul,
        server_opts => opts(presence_server),
        session_opts => opts(presence_session)
    },
    DiscoveryOpts = #{
        name => Nodename,
        consul => Consul,
        opts => opts(discovery)
    },
    {ok, _} = consuela_presence_sup:start_link(PresenceOpts),
    {ok, _} = consuela_discovery_sup:start_link(DiscoveryOpts),
    ok = await(Lifetime),
    init:stop().

await(Lifetime) ->
    ok = os_signal_relay:replace([sigterm]),
    receive
        {signal, sigterm} ->
            _ = logger:info("sigterm received, terminating ...", []),
            ok
    after Lifetime * 1000 -> ok
    end.

opts(Producer) ->
    #{pulse => {?MODULE, Producer}}.

%%

-spec handle_beat(consuela_client:beat() | consuela_discovery_server:beat(), atom()) -> ok.
handle_beat(Beat, Producer) ->
    logger:info("[~p] ~p", [Producer, Beat]).
