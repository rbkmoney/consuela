%%%
%%% Testing basic guarantees

-module(otp_registry_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-export([nonexistent_gives_noproc/1]).
-export([start_stop_works/1]).
-export([conflict_gives_already_started/1]).
-export([instant_reregister_succeeds/1]).

%% Pulse

-export([handle_beat/2]).

%% GenServer

-behaviour(gen_server).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Description

-spec all() -> [{group, group_name(), list()} | test_name()].
all() ->
    [
        nonexistent_gives_noproc,
        start_stop_works,
        conflict_gives_already_started,

        {group, instant_reregister, [{repeat, 10}]}
    ].

-spec groups() -> [{group_name(), list(), [test_name()]}].
groups() ->
    [
        {instant_reregister, [], [instant_reregister_succeeds]}
    ].

%% Startup / shutdown

-spec init_per_suite(config()) -> config().

-spec end_per_suite(config()) -> _.

init_per_suite(C) ->
    Apps0 = genlib_app:start_application(hackney),
    ok = ct_consul:await_ready(),
    Apps1 = genlib_app:start_application_with(consuela, [
        {registry, #{
            nodename => "consul0",
            namespace => <<?MODULE_STRING>>,
            consul => #{opts => #{pulse => {?MODULE, {client, debug}}}},
            keeper => #{pulse => {?MODULE, {keeper, info}}},
            reaper => #{pulse => {?MODULE, {reaper, info}}},
            registry => #{pulse => {?MODULE, {registry, info}}}
        }}
    ]),
    [{suite_apps, Apps0 ++ Apps1} | C].

end_per_suite(C) ->
    genlib_app:test_application_stop(?config(suite_apps, C)).

%% Definitions

-spec nonexistent_gives_noproc(config()) -> _.
-spec start_stop_works(config()) -> _.
-spec conflict_gives_already_started(config()) -> _.
-spec instant_reregister_succeeds(config()) -> _.

nonexistent_gives_noproc(_C) ->
    ?assertEqual({error, noproc}, try_call(mk_ref(noone), say)).

start_stop_works(_C) ->
    St = {just, "some", <<"term">>},
    Ref = mk_ref(meself),
    ?assertMatch({ok, _}, gen_server:start(Ref, ?MODULE, St, [])),
    ?assertEqual(St, gen_server:call(Ref, state)),
    ?assertEqual(St, sys:get_state(Ref)),
    ?assertEqual(ok, gen_server:stop(Ref)),
    ?assertEqual({error, noproc}, try_call(Ref, state)).

conflict_gives_already_started(_C) ->
    Ref = mk_ref(maself),
    {ok, Pid} = gen_server:start(Ref, ?MODULE, undefined, []),
    ?assertEqual({error, {already_started, Pid}}, gen_server:start(Ref, ?MODULE, undefined, [])),
    ?assertEqual(stopped, gen_server:call(Ref, stop)),
    ok.

instant_reregister_succeeds(_C) ->
    Ref = mk_ref(mooself),
    ?assertMatch({ok, _}, gen_server:start(Ref, ?MODULE, undefined, [])),
    ?assertEqual(stopped, gen_server:call(Ref, stop)),
    % strictly less than typical Consul request latency
    ok = timer:sleep(1),
    ?assertMatch({ok, _}, gen_server:start(Ref, ?MODULE, undefined, [])),
    ?assertEqual(stopped, gen_server:call(Ref, stop)).

try_call(Ref, Call) ->
    try gen_server:call(Ref, Call) of
        Result -> {ok, Result}
    catch
        exit:noproc -> {error, noproc};
        exit:{noproc, {gen_server, call, _}} -> {error, noproc}
    end.

mk_ref(Name) ->
    {via, consuela, Name}.

%%

-type st() :: _.

-spec init(st()) -> st().
init(St) ->
    {ok, St}.

-spec handle_call(state, {pid(), reference()}, st()) -> {reply, st(), st()}.
handle_call(state, _From, St) ->
    {reply, St, St};
handle_call(stop, _From, St) ->
    {stop, shutdown, stopped, St}.

-spec handle_cast(_, st()) -> {noreply, st()}.
handle_cast(_Msg, St) ->
    {noreply, St}.

-spec handle_info(_, st()) -> {noreply, st()}.
handle_info(_Info, St) ->
    {noreply, St}.

-spec terminate(_, st()) -> ok.
terminate(_Reason, _St) ->
    ok.

-spec code_change(_, st(), _) -> {ok, st()}.
code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%

-type category() :: atom().

-spec handle_beat
    (consuela_client:beat(), {client, category()}) -> ok;
    (consuela_session_keeper:beat(), {keeper, category()}) -> ok;
    (consuela_zombie_reaper:beat(), {reaper, category()}) -> ok;
    (consuela_registry:beat(), {registry, category()}) -> ok.
handle_beat(Beat, {Producer, Category}) ->
    ct:pal(Category, "[~p] ~p ~p", [Producer, self(), Beat]);
handle_beat(_Beat, _) ->
    ok.
