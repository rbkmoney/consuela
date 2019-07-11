%%%
%%% Testing basic guarantees

-module(basic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-type group_name() :: atom().
-type test_name()  :: atom().
-type config()     :: [{atom(), _}].

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([empty_lookup_notfound/1]).
-export([empty_unregistration_notfound/1]).
-export([registration_persists/1]).
-export([complex_process_name_ok/1]).
-export([conflicting_registration_fails/1]).
-export([registration_unregistration_succeeds/1]).
-export([conflicting_unregistration_fails/1]).
-export([dead_registration_cleaned/1]).
-export([registrations_select_ok/1]).

-export([unavail_lookup_exits/1]).
-export([unavail_registration_exits/1]).

%% Pulse

-export([handle_beat/2]).

%% Description

-spec all() ->
    [test_name() | {group, group_name()}].

all() ->
    [
        {group, regular_workflow},
        {group, proper_exceptions},
        {group, presence_workflow}
    ].

-spec groups() ->
    [{group_name(), list(_), [test_name()]}].

groups() ->
    [

        {regular_workflow, [parallel], [
            empty_lookup_notfound,
            empty_unregistration_notfound,
            registration_persists,
            complex_process_name_ok,
            conflicting_registration_fails,
            registration_unregistration_succeeds,
            conflicting_unregistration_fails,
            dead_registration_cleaned,
            registrations_select_ok
        ]},

        {proper_exceptions, [parallel], [
            unavail_lookup_exits,
            unavail_registration_exits
        ]},

        {presence_workflow, [], [
            {group, regular_workflow}
        ]}

    ].

%% Startup / shutdown

-spec init_per_suite(config()) ->
    config().

-spec end_per_suite(config()) ->
    _.

-spec init_per_group(group_name(), config()) ->
    config().

-spec end_per_group(group_name(), config()) ->
    _.

-spec init_per_testcase(test_name(), config()) ->
    config().

-spec end_per_testcase(test_name(), config()) ->
    _.

init_per_suite(C) ->
    Apps =
        genlib_app:start_application(ranch) ++
        genlib_app:start_application(consuela),
    ok = ct_consul:await_ready(),
    Consul = #{
        url  => "http://consul0:8500",
        opts => #{
            pulse => {?MODULE, {client, debug}}
        }
    },
    [{suite_apps, Apps}, {consul, Consul} | C].

end_per_suite(C) ->
    genlib_app:test_application_stop(?config(suite_apps, C)).

init_per_group(proper_exceptions, C) ->
    {ok, Proxy = #{endpoint := {Host, Port}}} = ct_proxy:start_link({"consul0", 8500}),
    Consul = #{
        url   => ["http://", Host, ":", integer_to_list(Port)],
        opts => #{
            transport_opts => #{
                pool            => false,
                connect_timeout => 100,
                recv_timeout    => 1000
            },
            pulse => {?MODULE, {client, debug}}
        }
    },
    [{proxy, ct_proxy:unlink(Proxy)}, {consul, Consul} | C];
init_per_group(presence_workflow, C) ->
    Name = <<?MODULE_STRING>>,
    Opts = #{
        name         => Name,
        consul       => ?config(consul, C),
        server_opts  => #{pulse => {?MODULE, {presence_server, info}}},
        session_opts => #{pulse => {?MODULE, {presence_session, info}}}
    },
    {ok, Pid} = consuela_presence_sup:start_link(Opts),
    [{session, #{presence => Name}}, {presence_sup, ct_helper:unlink(Pid)} | C];
init_per_group(_, C) ->
    C.

end_per_group(proper_exceptions, C) ->
    ct_proxy:stop(?config(proxy, C));
end_per_group(presence_workflow, C) ->
    consuela_presence_sup:stop(?config(presence_sup, C));
end_per_group(_, _C) ->
    ok.

init_per_testcase(Name, C) ->
    Opts = #{
        nodename  => "consul0",
        namespace => genlib:to_binary(Name),
        consul    => ?config(consul, C),
        session   => proplists:get_value(session, C, #{}),
        keeper    => #{pulse => {?MODULE, {keeper, info}}},
        reaper    => #{pulse => {?MODULE, {reaper, info}}},
        registry  => #{pulse => {?MODULE, {registry, info}}}
    },
    {ok, Pid} = consuela_registry_sup:start_link(Name, Opts),
    [{registry, Name}, {registry_sup, Pid}, {testcase, Name} | C].

end_per_testcase(_Name, C) ->
    catch change_proxy_mode(pass, C),
    catch consuela_registry_sup:stop(?config(registry_sup, C)).

%% Definitions

-spec empty_lookup_notfound(config())                -> _.
-spec empty_unregistration_notfound(config())        -> _.
-spec registration_persists(config())                -> _.
-spec complex_process_name_ok(config())              -> _.
-spec conflicting_registration_fails(config())       -> _.
-spec registration_unregistration_succeeds(config()) -> _.
-spec conflicting_unregistration_fails(config())     -> _.
-spec dead_registration_cleaned(config())            -> _.
-spec registrations_select_ok(config())              -> _.

-spec unavail_lookup_exits(config())                 -> _.
-spec unavail_registration_exits(config())           -> _.

empty_lookup_notfound(C) ->
    Ref = ?config(registry, C),
    ?assertEqual({error, notfound}, lookup(Ref, me)).

empty_unregistration_notfound(C) ->
    Ref = ?config(registry, C),
    ?assertEqual({error, notfound}, unregister(Ref, me, self())).

registration_persists(C) ->
    Ref = ?config(registry, C),
    _ = ?assertEqual(ok, register(Ref, me, self())),
    _ = ?assertEqual(ok, register(Ref, me, self())),
    _ = ?assertEqual({ok, self()}, lookup(Ref, me)),
    ok.

conflicting_registration_fails(C) ->
    Ref = ?config(registry, C),
    Pid1 = spawn_slacker(),
    Pid2 = spawn_slacker(),
    _ = ?assertEqual(ok, register(Ref, my_boy, Pid1)),
    _ = ?assertEqual({error, exists}, register(Ref, my_boy, Pid2)),
    _ = ?assertEqual(ok, register(Ref, my_boy, Pid1)),
    ok.

complex_process_name_ok(C) ->
    Ref = ?config(registry, C),
    Name = {some, [<<"complex">>, 36#NAME]},
    _ = ?assertEqual(ok, register(Ref, Name, self())),
    _ = ?assertEqual({ok, self()}, lookup(Ref, Name)),
    ok.

registration_unregistration_succeeds(C) ->
    Ref = ?config(registry, C),
    _ = ?assertEqual(ok, register(Ref, me, self())),
    _ = ?assertEqual({ok, self()}, lookup(Ref, me)),
    _ = ?assertEqual(ok, unregister(Ref, me, self())),
    _ = ?assertEqual({error, notfound}, unregister(Ref, me, self())),
    _ = ?assertEqual({error, notfound}, lookup(Ref, me)),
    ok.

conflicting_unregistration_fails(C) ->
    Ref = ?config(registry, C),
    _ = ?assertEqual(ok, register(Ref, me, self())),
    _ = ?assertEqual({error, notfound}, unregister(Ref, me, erlang:whereis(kernel_sup))),
    ok.

dead_registration_cleaned(C) ->
    Ref = ?config(registry, C),
    Pid = spawn_slacker(),
    _ = ?assertEqual(ok, register(Ref, my_boy, Pid)),
    ok = stop_slacker(Pid),
    _ = ct_helper:await({error, notfound}, fun () -> lookup(Ref, my_boy) end),
    _ = ?assertEqual({error, notfound}, unregister(Ref, my_boy, Pid)),
    ok.

registrations_select_ok(C) ->
    N = 10,
    Ref = ?config(registry, C),
    Slackers = [{I, spawn_slacker()} || I <- lists:seq(1, N)],
    _ = [?assertEqual(ok, register(Ref, I, Pid)) || {I, Pid} <- Slackers],
    _ = ?assertEqual(Slackers, lists:sort(consuela_registry_server:all(Ref))),
    _ = [?assertEqual(ok, unregister(Ref, I, Pid)) || {I, Pid} <- Slackers],
    _ = [?assertEqual(ok, stop_slacker(Pid)) || {_, Pid} <- Slackers],
    ok.

unavail_lookup_exits(C) ->
    Ref = ?config(registry, C),
    ok = change_proxy_mode(ignore, C),
    ?assertExit(
        {consuela, {unknown, {transport_error, timeout}}},
        lookup(Ref, my_boy)
    ).

unavail_registration_exits(C) ->
    Ref = ?config(registry, C),
    ok = change_proxy_mode(ignore, C),
    ?assertExit(
        {consuela, {unknown, timeout}},
        register(Ref, my_boy, self())
    ).

spawn_slacker() ->
    erlang:spawn_link(fun () -> receive after infinity -> ok end end).

stop_slacker(Pid) ->
    ct_helper:stop_linked(Pid, shutdown).

register(Ref, Name, Pid) ->
    consuela_registry_server:register(Ref, Name, Pid).

unregister(Ref, Name, Pid) ->
    consuela_registry_server:unregister(Ref, Name, Pid).

lookup(Ref, Name) ->
    consuela_registry_server:lookup(Ref, Name).

change_proxy_mode(Mode, C) ->
    Proxy = ?config(proxy, C),
    {ok, ModeWas} = ct_proxy:mode(Proxy, Mode),
    _ = ct:pal(debug, "[~p] set proxy from '~p' to '~p'", [?config(testcase, C), ModeWas, Mode]),
    ok.

%%

-type category() :: atom().

-spec handle_beat
    (consuela_client:beat(), {client, category()}) -> ok;
    (consuela_session_keeper:beat(), {keeper, category()}) -> ok;
    (consuela_zombie_reaper:beat(), {reaper, category()}) -> ok;
    (consuela_registry:beat(), {registry, category()}) -> ok
.

handle_beat(Beat, {Producer, Category}) ->
    ct:pal(Category, "[~p] ~p", [Producer, Beat]);
handle_beat(_Beat, _) ->
    ok.
