%%%
%%% Testing zombie reaper behaves well

-module(reaper_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-type group_name() :: atom().
-type test_name()  :: atom().
-type config()     :: [{atom(), _}].

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([zombie_reaping_succeeded/1]).
-export([reaper_dies_eventually/1]).
-export([reaper_queue_drains_eventually/1]).

%% Pulse

-export([handle_beat/2]).

%% Description

-spec all() ->
    [test_name() | {group, group_name()}].

all() ->
    [
        {group, regular_workflow}
    ].

-spec groups() ->
    [{group_name(), list(_), [test_name()]}].

groups() ->
    [
        {regular_workflow, [parallel], [
            zombie_reaping_succeeded,
            reaper_queue_drains_eventually,
            reaper_dies_eventually
        ]}
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
    Apps =
        genlib_app:start_application(ranch) ++
        ct_helper:ensure_app_loaded(consuela),
    ok = ct_consul:await_ready(),
    [{suite_apps, Apps} | C].

end_per_suite(C) ->
    genlib_app:test_application_stop(?config(suite_apps, C)).

init_per_testcase(Name, C) ->
    {ok, Proxy = #{endpoint := {Host, Port}}} = ct_proxy:start_link({"consul0", 8500}),
    Self = self(),
    Opts = #{
        nodename  => "consul0",
        namespace => genlib:to_binary(Name),
        consul    => #{
            url   => ["http://", Host, ":", integer_to_list(Port)],
            opts  => #{
                transport_opts => #{
                    pool            => false,
                    connect_timeout => 100,
                    recv_timeout    => 1000
                },
                pulse          => {?MODULE, {client, #{log => debug}}}
            }
        },
        keeper    => #{
            pulse => {?MODULE, {keeper, #{log => info}}}
        },
        reaper    => #{
            retry => genlib_retry:exponential(3, 2, 100),
            pulse => {?MODULE, {reaper, #{log => info, relay => Self}}}
        },
        registry  => #{
            pulse => {?MODULE, {registry, #{log => info}}}
        }
    },
    {ok, Pid} = consuela_registry_sup:start_link(Name, Opts),
    [{registry, Name}, {registry_sup, Pid}, {proxy, Proxy} | C].

end_per_testcase(_Name, C) ->
    _ = (catch consuela_registry_sup:stop(?config(registry_sup, C))),
    _ = (catch ct_proxy:stop(?config(proxy, C))),
    ok.

%% Definitions

-define(assertReceive(__Expr),
    ?assertReceive(__Expr, 1000)
).

-define(assertReceive(__Expr, __Timeout), (begin
    receive (__Expr) -> ok after (__Timeout) ->
        erlang:error({assertReceive, [
            {module, ?MODULE},
            {line, ?LINE},
            {expression, (??__Expr)}
        ]})
    end
end)).

-spec zombie_reaping_succeeded(config()) -> _.
-spec reaper_dies_eventually(config()) -> _.
-spec reaper_queue_drains_eventually(config()) -> _.

zombie_reaping_succeeded(C) ->
    Ref = ?config(registry, C),
    Proxy = ?config(proxy, C),
    Pid = spawn_slacker(),
    _ = ?assertEqual(ok, register(Ref, boi, Pid)),
    _ = ?assertEqual({ok, pass}, ct_proxy:mode(Proxy, stop)),
    _ = ?assertEqual({ok, Pid}, lookup(Ref, boi)),
    _ = ?assertEqual(ok, stop_slacker(Pid)),
    _ = ?assertReceive({reaper, {{zombie, {_, boi, Pid}}, enqueued}}),
    _ = ?assertReceive({reaper, {{timer, _}, {started, 100}}}),
    _ = ?assertEqual({ok, stop}, ct_proxy:mode(Proxy, pass)),
    _ = ?assertReceive({reaper, {{timer, _}, fired}}),
    _ = ?assertReceive({reaper, {{zombie, {_, boi, Pid}}, {reaping, succeeded}}}),
    _ = ?assertEqual({error, notfound}, lookup(Ref, boi)),
    ok.

reaper_dies_eventually(C) ->
    Ref = ?config(registry, C),
    Sup = ?config(registry_sup, C),
    Proxy = ?config(proxy, C),
    Pid = spawn_slacker(),
    Flag = erlang:process_flag(trap_exit, true),
    _ = ?assertEqual(ok, register(Ref, boi, Pid)),
    _ = ?assertEqual({ok, pass}, ct_proxy:mode(Proxy, stop)),
    _ = ?assertEqual(ok, stop_slacker(Pid)),
    _ = ?assertReceive({reaper, {{zombie, {_, boi, Pid}}, enqueued}}),
    _ = ?assertReceive({reaper, {{timer, _}, {started, 100}}}),
    _ = ?assertReceive({reaper, {{timer, _}, fired}}),
    _ = ?assertReceive({reaper, {{zombie, {_, boi, Pid}}, {reaping, {failed, {unknown, _}}}}}),
    _ = ?assertReceive({reaper, {{timer, _}, {started, 200}}}),
    _ = ?assertReceive({reaper, {{timer, _}, fired}}),
    _ = ?assertReceive({reaper, {{zombie, {_, boi, Pid}}, {reaping, {failed, {unknown, _}}}}}),
    _ = ?assertReceive({reaper, {{timer, _}, {started, 400}}}),
    _ = ?assertReceive({reaper, {{timer, _}, fired}}),
    _ = ?assertReceive({reaper, {{zombie, {_, boi, Pid}}, {reaping, {failed, {unknown, _}}}}}),
    _ = ?assertReceive({'EXIT', Sup, _Reason}),
    _ = erlang:process_flag(trap_exit, Flag),
    ok.

reaper_queue_drains_eventually(C) ->
    N = 20,
    Ref = ?config(registry, C),
    Proxy = ?config(proxy, C),
    Slackers = [{I, spawn_slacker()} || I <- lists:seq(1, N)],
    _ = genlib_pmap:map(
        fun ({I, Pid}) -> ?assertEqual(ok, register(Ref, I, Pid)) end,
        Slackers
    ),
    _ = ?assertEqual({ok, pass}, ct_proxy:mode(Proxy, stop)),
    _ = [?assertEqual(ok, stop_slacker(Pid)) || {_, Pid} <- Slackers],
    _ = timer:sleep(100),
    _ = ?assertEqual({ok, stop}, ct_proxy:mode(Proxy, ignore)),
    _ = timer:sleep(100),
    _ = ?assertEqual({ok, ignore}, ct_proxy:mode(Proxy, pass)),
    _ = [
        begin
            _ = ?assertReceive({reaper, {{zombie, {_, I, Pid}}, enqueued}}, N * 100),
            _ = ?assertReceive({reaper, {{zombie, {_, I, Pid}}, {reaping, succeeded}}}, N * 100),
            ok
        end || {I, Pid} <- Slackers
    ],
    _ = genlib_pmap:map(
        fun ({I, _}) -> ?assertEqual({error, notfound}, lookup(Ref, I)) end,
        Slackers
    ),
    ok.


register(Ref, Name, Pid) ->
    consuela_registry_server:register(Ref, Name, Pid).

lookup(Ref, Name) ->
    consuela_registry_server:lookup(Ref, Name).

spawn_slacker() ->
    erlang:spawn_link(fun () -> receive after infinity -> ok end end).

stop_slacker(Pid) ->
    ct_helper:stop_linked(Pid, shutdown).

%%

-type category() :: atom().
-type opts() :: #{log => category(), relay => pid()}.

-spec handle_beat
    (consuela_client:beat(), {client, opts()}) -> ok;
    (consuela_session_keeper:beat(), {keeper, opts()}) -> ok;
    (consuela_zombie_reaper:beat(), {reaper, opts()}) -> ok;
    (consuela_registry:beat(), {registry, opts()}) -> ok
.

handle_beat(Beat, {Producer, Opts}) ->
    genlib_map:foreach(
        fun
            (log, Category) -> ct:pal(Category, "[~p] ~p", [Producer, Beat]);
            (relay, Pid)    -> Pid ! {Producer, Beat}
        end,
        Opts
    ).