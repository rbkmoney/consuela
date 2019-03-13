%%%
%%% Testing discovery mechanism
%%%
%%% NOTES
%%%
%%% Most of the SUT setup happens through docker compose orchestration, see `start-discovery-node.sh`. We only
%%% consider checking that the whole three-node cluster formed without manual intervention.

-module(discovery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-type group_name() :: atom().
-type test_name()  :: atom().
-type config()     :: [{atom(), _}].

-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-export([nodes_discover_themselves/1]).

%% Pulse

-export([handle_beat/2]).

%% Description

-spec all() ->
    [test_name() | {group, group_name()}].

all() ->
    [
        nodes_discover_themselves
    ].

%% Startup / shutdown

-include_lib("kernel/include/inet.hrl").

-spec init_per_suite(config()) ->
    config().

-spec end_per_suite(config()) ->
    _.

init_per_suite(C) ->
    {ok, #hostent{h_addr_list = [Address | _]}} = inet:gethostbyname(inet_db:gethostname()),
    Nodename = list_to_atom(?MODULE_STRING ++ "@" ++ inet:ntoa(Address)),
    {ok, _Pid} = net_kernel:start([Nodename, longnames]),
    true = erlang:set_cookie(node(), discovery),
    Apps = ct_helper:ensure_app_loaded(consuela),
    ok = ct_consul:await_ready(),
    [{n, 3}, {service, <<"discovery">>}, {apps, Apps} | C].

end_per_suite(C) ->
    genlib_app:test_application_stop(?config(apps, C)).

%% Definitions

-include("ct_helper.hrl").

-spec nodes_discover_themselves(config()) -> _.

nodes_discover_themselves(C) ->
    N = ?config(n, C),
    Service = ?config(service, C),
    Client = consuela_client:new("http://consul0:8500", #{pulse => mk_pulse(client, debug)}),
    [#{service := #{endpoint := {Address, _}}} | _Rest] = await_healthy_service(N, Service, Client),
    Node = list_to_atom(binary_to_list(Service) ++ "@" ++ inet:ntoa(Address)),
    true = net_kernel:hidden_connect_node(Node),
    _Nodes = await_known_nodes(N, Node),
    ok.

await_healthy_service(N, Service, Client) ->
    ct_helper:await_n(
        N, fun () ->
            case consuela_health:get(Service, [], true, Client) of
                {ok, Hs} -> Hs;
                Other    -> Other
            end
        end,
        genlib_retry:linear(3, 5000)
    ).

await_known_nodes(N, Node) ->
    ct_helper:await_n(
        N,
        fun () -> rpc:call(Node, erlang, nodes, [[this, visible]]) end,
        genlib_retry:linear(3, 5000)
    ).

%%

-type opts() :: #{log => atom()}.

mk_pulse(Producer, Category) ->
    {?MODULE, {Producer, #{log => Category}}}.

-spec handle_beat
    (consuela_client:beat(), {client, opts()}) -> ok
.

handle_beat(Beat, {Producer, Opts}) ->
    genlib_map:foreach(
        fun
            (log, Category) -> ct:pal(Category, "[~p] ~9999p", [Producer, Beat])
        end,
        Opts
    ).
