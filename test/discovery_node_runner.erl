-module(discovery_node_runner).
-include_lib("kernel/include/inet.hrl").

-export([run/4]).

-export([handle_beat/2]).

%%

-spec run(binary(), inet:hostname(), consuela_client:url(), pos_integer()) ->
    ok.

run(Nodename, Hostname, ConsulUrl, Lifetime) ->
    ok     = logger:set_primary_config(level, info),
    _Apps  = ct_helper:ensure_app_loaded(consuela),
    ok     = ct_consul:await_ready(),
    Client = consuela_client:new(ConsulUrl, opts(client)),
    ok     = register(Nodename, Hostname, Lifetime, Client),
    try
        {ok, _Pid} = consuela_discovery_server:start_link(Nodename, [], Client, opts(discovery)),
        ok = await(Lifetime)
    after
        ok = deregister(Nodename, Client)
    end,
    init:stop().

register(Nodename, Hostname, Lifetime, Client) ->
    {ok, #hostent{h_addr_list = [IP | _]}} = inet:gethostbyname(Hostname),
    consuela_health:register(
        #{
            name     => Nodename,
            tags     => [],
            endpoint => {IP, 31337},
            checks   => [#{
                name    => <<Nodename/binary, ":erlang-node">>,
                type    => {ttl, Lifetime},
                initial => passing
            }]
        },
        Client
    ).

deregister(Nodename, Client) ->
    consuela_health:deregister(Nodename, Client).

await(Lifetime) ->
    ok = os_signal_relay:replace([sigterm]),
    receive
        {signal, sigterm} ->
            _ = logger:info("sigterm received, terminating ...", []),
            ok
    after
        Lifetime * 1000 ->
            ok
    end.

opts(Producer) ->
    #{pulse => {?MODULE, Producer}}.

%%

-spec handle_beat(consuela_client:beat() | consuela_discovery_server:beat(), atom()) ->
    ok.

handle_beat(Beat, Producer) ->
    logger:info("[~p] ~p", [Producer, Beat]).
