%%%
%%% Testing zombie reaper behaves well

-module(leader_sup_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-type group_name() :: atom().
-type test_name()  :: atom().
-type config()     :: [{atom(), _}].

-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([leader_uniqueness_holds/1]).
-export([leader_liveness_holds/1]).

%% Pulse

-export([handle_beat/2]).

%% Supervisor

-behaviour(supervisor).
-export([init/1]).

%% Description

-spec all() ->
    [test_name() | {group, group_name()}].

all() ->
    [
        leader_uniqueness_holds,
        leader_liveness_holds
    ].

%% Startup / shutdown

-spec init_per_suite(config()) ->
    config().

-spec end_per_suite(config()) ->
    _.

-spec init_per_testcase(test_name(), config()) ->
    config().

-spec end_per_testcase(test_name(), config()) ->
    _.

init_per_suite(C) ->
    {ok, _Pid} = net_kernel:start([?MODULE, shortnames]),
    true = erlang:set_cookie(node(), ?MODULE),
    Apps = ct_helper:ensure_app_loaded(consuela),
    ok = ct_consul:await_ready(),
    [{apps, Apps} | C].

end_per_suite(C) ->
    genlib_app:test_application_stop(?config(apps, C)).

init_per_testcase(Name, C) ->
    N = 3,
    CodePath = [P || P <- code:get_path(), filelib:is_dir(P)],
    Nodes = genlib_pmap:map(fun (I) -> start_slave(mk_nodename(Name, I), CodePath) end, lists:seq(1, N)),
    [{nodes, Nodes} | C].

end_per_testcase(_Name, C) ->
    _ = genlib_pmap:map(fun (N) -> ok = stop_slave(N) end, ?config(nodes, C)),
    ok.

mk_nodename(Prefix, N) ->
    binary_to_atom(genlib:format("~s_~p", [Prefix, N]), latin1).

start_slave(Name, CodePath) ->
    {ok, Node} = slave:start(net_adm:localhost(), Name, "-setcookie " ++ ?MODULE_STRING),
    true = rpc:call(Node, code, set_path, [CodePath]),
    _Apps = [_ | _] = rpc:call(Node, genlib_app, start_application_with, [consuela, [
        {nodename  , "consul0"},
        {namespace , <<?MODULE_STRING>>},
        {registry  , #{pulse => mk_pulse(registry)}}
    ]]),
    Node.

stop_slave(Node) ->
    case rpc:call(Node, init, stop, []) of
        ok ->
            _ = ct_helper:await(pang, fun () -> net_adm:ping(Node) end, genlib_retry:linear(10, 500)),
            ok;
        {badrpc,nodedown} ->
            ok
    end.

%% Definitions

-include("ct_helper.hrl").

-spec leader_uniqueness_holds(config()) -> _.
-spec leader_liveness_holds(config()) -> _.

leader_uniqueness_holds(C) ->
    Name = ?FUNCTION_NAME,
    Nodes = [Node | _] = ?config(nodes, C),
    ok = start_leaders(Nodes, [Name, ?MODULE, {dummy, Name}, #{pulse => mk_pulse(leadersup)}]),
    [{dummy, Pid, _, _}] = rpc:call(Node, consuela_leader_supervisor, which_children, [Name]),
    {Results, []} = rpc:multicall(Nodes, erlang, whereis, [Name]),
    [Pid | Rest] = lists:reverse(lists:sort(Results)), % pid() > atom()
    _ = ?assert(is_pid(Pid)),
    _ = [?assertEqual(undefined, E) || E <- Rest],
    ok.

leader_liveness_holds(C) ->
    Name = ?FUNCTION_NAME,
    TTL = 60,
    Nodes0 = ?config(nodes, C),
    Client = consuela_client:new("http://consul0:8500", #{}),
    {ok, Session} = consuela_session:create(genlib:to_binary(Name), "consul0", TTL, Client),
    try
        Opts = #{
            retry => genlib_retry:linear(1, 500),
            pulse => {?MODULE, {leadersup, #{logger => node(), relay => self()}}}
        },
        ok = start_leaders(Nodes0, [Name, ?MODULE, {holder, Name, Session, Client}, Opts]),
        ok = leader_liveness_holds(Name, Nodes0, fun () -> is_live(Name, Client) end),
        ok
    after
        consuela_session:destroy(Session, Client)
    end.

leader_liveness_holds(Name, Nodes = [_ | _], IsLive) ->
    N1 = length(Nodes) - 1,
    Pid1 = wait_leader(Name),
    ok = IsLive(),
    _ = [?assertReceive({leadersup, {{warden, Name}, {started, _}}})           || _ <- lists:seq(1, N1)],
    Nodes1 = stop_leader(Pid1, Nodes),
    _ = [?assertReceive({leadersup, {{leader, Name}, {down, Pid1, shutdown}}}) || _ <- lists:seq(1, N1)],
    _ = [?assertReceive({leadersup, {{warden, Name}, {stopped, _}}}, 3000)     || _ <- lists:seq(1, N1)],
    leader_liveness_holds(Name, Nodes1, IsLive);
leader_liveness_holds(_Name, [], _IsLive) ->
    ok.

is_live(Name, Client) ->
    _ = ?assertMatch({ok, _}, ct_lock_holder:get(Name, Client)),
    ok.

wait_leader(Name) ->
    receive
        {leadersup, {{leader, Name}, {started, Pid}}} -> Pid
    after
        3000 -> erlang:error({no_leader_found, Name})
    end.

start_leaders(Nodes, Args) ->
    {OkPids, []} = rpc:multicall(Nodes, supervisor, start_child, [kernel_safe_sup, #{
        id    => leader,
        start => {consuela_leader_supervisor, start_link, Args}
    }]),
    _ = [?assertMatch({ok, _}, E) || E <- OkPids],
    ok.

stop_leader(Pid, Nodes) ->
    Node = node(Pid),
    _ = ?assert(lists:member(Node, Nodes)),
    ok = stop_slave(Node),
    Nodes -- [Node].

%% Supervisor

-spec init(_Args) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

init({dummy, Name}) ->
    {ok, {#{}, [
        #{
            id    => dummy,
            start => {genlib_adhoc_supervisor, start_link, [{local, Name}, #{}, []]}
        }
    ]}};
init({holder, Name, Session, Client}) ->
    {ok, {#{}, [
        #{
            id    => keeper,
            start => {ct_lock_holder, start_link, [Name, Session, Client]}
        }
    ]}}.

%%

-type opts() :: #{logger => node(), relay => pid()}.

mk_pulse(Producer) ->
    {?MODULE, {Producer, #{logger => node()}}}.

-spec handle_beat
    (consuela_client:beat(), {client, opts()}) -> ok;
    (consuela_session_keeper:beat(), {keeper, opts()}) -> ok;
    (consuela_zombie_reaper:beat(), {reaper, opts()}) -> ok;
    (consuela_registry:beat(), {registry, opts()}) -> ok
.

handle_beat(Beat, {Producer, Opts}) ->
    genlib_map:foreach(
        fun
            (logger, Node) -> rpc:call(Node, ct, pal, ["<~p> [~p] ~9999p", [node(), Producer, Beat]]);
            (relay , Pid ) -> Pid ! {Producer, Beat}
        end,
        Opts
    ).
