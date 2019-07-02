%%%
%%% Discovery service
%%%
%%% NOTES
%%%
%%% We are knowingly relying on someone else to maintain service registrations in Consul, we only consider
%%% ourself with service instances and their health.

-module(consuela_discovery_server).

%%

-type service()   :: consuela_health:servicename().
-type tag()       :: consuela_health:tag().
-type client()    :: consuela_client:t().
-type millisecs() :: pos_integer().

-type opts() :: #{
    interval => #{
        init => millisecs(),
        idle => millisecs()
    },
    pulse    => {module(), _PulseOpts}
}.

-export_type([service/0]).
-export_type([tag/0]).
-export_type([opts/0]).

-export([start_link/4]).

%% gen server

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%%

-type beat() ::
    {discovery, started | {failed, _Reason} | {succeeded, [node()]}} |
    {{connect, node()}, started | {finished, boolean()}} |
    {{timer, reference() | undefined}, {started, timeout()} | fired} |
    {{node, node()}, up | {down, _Reason}} |
    {unexpected, {{call, from()} | cast | info, _Msg}}.

-export_type([beat/0]).

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

-spec start_link(service(), [tag()], client(), opts()) ->
    {ok, pid()}.

start_link(Name, Tags, Client, Opts) ->
    St = mk_state(Name, Tags, Client, Opts),
    gen_server:start_link(?MODULE, St, []).

%%

-type st() :: #{
    service  := service(),
    tags     := [tag()],
    interval := #{init := millisecs(), idle := millisecs()},
    tref     => reference(),
    nodes    => [consuela_health:nodename()],
    client   := client(),
    pulse    := {module(), _PulseOpts}
}.

-type from() :: {pid(), reference()}.

-spec mk_state(service(), [tag()], client(), opts()) ->
    st().

mk_state(Name, Tags, Client, Opts) ->
    #{
        service  => Name,
        tags     => Tags,
        interval => maps:merge(maps:get(interval, Opts, #{}), #{
            init =>   5000,
            idle => 600000
        }),
        client   => Client,
        pulse    => maps:get(pulse, Opts, {?MODULE, []})
    }.

-spec init(st()) ->
    {ok, st(), 0}.

init(St) ->
    ok = net_kernel:monitor_nodes(true, [nodedown_reason]),
    {ok, St, 0}.

-spec handle_call(_Call, from(), st()) ->
    {noreply, st(), hibernate}.

handle_call(Call, From, St) ->
    _ = beat({unexpected, {{call, From}, Call}}, St),
    {noreply, St, hibernate}.

-spec handle_cast(_Cast, st()) ->
    {noreply, st(), hibernate}.

handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St, hibernate}.

-type info() ::
    timeout |
    {timeout, reference(), discover} |
    {nodeup, node(), []} |
    {nodedown, node(), [{nodedown_reason, _Reason}]}.

-spec handle_info(info(), st()) ->
    {noreply, st(), hibernate}.

handle_info(timeout, St) ->
    _ = beat({{timer, undefined}, fired}, St),
    {noreply, start_timer(try_discover(St)), hibernate};
handle_info({timeout, TRef, discover}, St = #{tref := TRef}) ->
    _ = beat({{timer, TRef}, fired}, St),
    {noreply, start_timer(try_discover(maps:remove(tref, St))), hibernate};
handle_info({nodeup, Node, _}, St) ->
    _ = beat({{node, Node}, up}, St),
    {noreply, handle_node_up(Node, St), hibernate};
handle_info({nodedown, Node, Info}, St) ->
    Reason = genlib_opts:get(nodedown_reason, Info),
    _ = beat({{node, Node}, {down, Reason}}, St),
    {noreply, handle_node_down(Node, St), hibernate};
handle_info(Info, St) ->
    _ = beat({unexpected, {info, Info}}, St),
    {noreply, St, hibernate}.

-spec terminate(_Reason, st()) ->
    ok.

terminate(_Reason, _St) ->
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.

%%

start_timer(St0 = #{}) ->
    false = maps:is_key(tref, St0),
    Timeout = compute_timeout(St0),
    TimerRef = consuela_timer:start(Timeout, discover),
    St1 = St0#{tref => TimerRef},
    _ = beat({{timer, TimerRef}, {started, Timeout}}, St1),
    St1.

compute_timeout(#{interval := #{idle := Timeout}, nodes := []}) ->
    % No nodes left to connect for now
    Timeout;
compute_timeout(#{interval := #{init := Timeout}, nodes := [_ | _]}) ->
    % There are nodes which are out of cluster still
    Timeout;
compute_timeout(#{interval := #{init := Timeout}}) ->
    % No known nodes at all
    Timeout.

try_discover(St) ->
    _ = beat({discovery, started}, St),
    case erlang:is_alive() of
        true ->
            discover(St);
        false ->
            _ = beat({discovery, {failed, nodistribution}}, St),
            St
    end.

discover(St = #{}) ->
    try get_service_health(St) of
        {ok, Hs} ->
            Nodes = collect_nodes(Hs),
            _ = beat({discovery, {succeeded, Nodes}}, St),
            NodesLeft = Nodes -- erlang:nodes([visible, this]),
            try_connect_nodes(St#{nodes => NodesLeft})
    catch
        error:{Class, _} = Error when Class == unknown; Class == failed ->
            _ = beat({discovery, {failed, Error}}, St),
            St
    end.

get_service_health(#{service := Service, tags := Tags, client := Client}) ->
    consuela_health:get(Service, Tags, true, Client).

collect_nodes(Hs) ->
    [mk_nodename(Address) ||
        % > https://www.consul.io/api/catalog.html#serviceaddress
        % > `ServiceAddress` is the IP address of the service host — if empty, node address should be used
        Address <- lists:usort([genlib:define(ServiceAddress, NodeAddress) ||
            #{
                node    := #{address := NodeAddress},
                service := #{endpoint := {ServiceAddress, _}}
            } <- Hs
        ])
    ].

try_connect_nodes(St = #{nodes := Nodes}) ->
    _ = genlib_pmap:map(fun (N) -> try_connect_node(N, St) end, Nodes),
    St.

try_connect_node(Node, St) ->
    _ = beat({{connect, Node}, started}, St),
    Result = net_kernel:connect_node(Node),
    _ = beat({{connect, Node}, {finished, Result}}, St),
    Result.

mk_nodename(Address) ->
    % We are silently assuming that any Erlang nodes around there that we may want to connect to, share _node
    % name_ with us. _Node name_ here is everything up to an '@'.
    Prefix = extract_nodename_prefix(erlang:atom_to_list(erlang:node())),
    erlang:list_to_atom(Prefix ++ "@" ++ inet:ntoa(Address)).

extract_nodename_prefix(Nodename) ->
    lists:takewhile(fun (C) -> C /= $@ end, Nodename).

handle_node_up(Node, St = #{nodes := Nodes}) ->
    % If we succeeded contacting all nodes known to Consul then the next timer timeout will be _Idle_ ms
    St#{nodes := Nodes -- [Node]};
handle_node_up(_Node, St = #{}) ->
    % This node may be linked up from another node before our very first discovery
    St.

handle_node_down(Node, St = #{nodes := Nodes}) ->
    % If we lose any node known earlier then the next timer timeout will be _Init_ ms
    St#{nodes := lists:usort([Node | Nodes])};
handle_node_down(_Node, St = #{}) ->
    St.

%%

-spec beat(beat(), st()) ->
    _.

beat(Beat, #{pulse := {Module, PulseOpts}}) ->
    % TODO handle errors?
    Module:handle_beat(Beat, PulseOpts).

-spec handle_beat(beat(), [trace]) ->
    ok.

handle_beat(Beat, [trace]) ->
    logger:debug("[~p] ~p", [?MODULE, Beat]);
handle_beat(_Beat, []) ->
    ok.
