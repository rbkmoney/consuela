%%%
%%% Testing basic guarantees

-module(multireg_SUITE).

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

-export([single_registration_succeeds/1]).

%% Pulse

-export([handle_beat/2]).

%% Description

-spec all() ->
    [{group, group_name(), list()}].

all() ->
    [
        {group, multireg, [{repeat, 20}]}
    ].

-spec groups() ->
    [{group_name(), list(), [test_name()]}].

groups() ->
    [
        {multireg, [], [single_registration_succeeds]}
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
    Apps = genlib_app:start_application(consuela),
    ok = ct_consul:await_ready(),
    [{suite_apps, Apps} | C].

end_per_suite(C) ->
    genlib_app:test_application_stop(?config(suite_apps, C)).

init_per_testcase(Name, C) ->
    Nodes = ["consul0", "consul1", "consul2"],
    BaseOpts = #{
        namespace => genlib:to_binary(Name),
        consul    => #{opts => #{pulse => {?MODULE, {client, debug}}}},
        keeper    => #{pulse => {?MODULE, {keeper, info}}},
        reaper    => #{pulse => {?MODULE, {reaper, info}}},
        registry  => #{pulse => {?MODULE, {registry, info}}}
    },
    {RegRefs, RegSups} = lists:unzip(
        [start_registry(N, Name, Node, BaseOpts) || {N, Node} <- ct_lists:enumerate(Nodes)]
    ),
    [{n, erlang:length(Nodes)}, {registry_sups, RegSups}, {registry_refs, RegRefs} | C].

start_registry(N, Name0, Node, BaseOpts) ->
    Name = mk_registry_name(Name0, N),
    {ok, Pid} = consuela_registry_sup:start_link(Name, BaseOpts#{nodename => Node}),
    {Name, Pid}.

mk_registry_name(Name, N) ->
    erlang:list_to_atom(erlang:atom_to_list(Name) ++ "_" ++ erlang:integer_to_list(N)).

end_per_testcase(_Name, C) ->
    [consuela_registry_sup:stop(Pid) || Pid <- ?config(registry_sups, C)].

%% Definitions

-spec single_registration_succeeds(config()) -> _.

single_registration_succeeds(C) ->
    RegRefs = ?config(registry_refs, C),
    Pid = self(),
    Results = genlib_pmap:map(fun (Ref) -> consuela_registry_server:register(Ref, me, Pid) end, RegRefs),
    _       = genlib_pmap:map(fun (Ref) -> consuela_registry_server:unregister(Ref, me, Pid) end, RegRefs),
    ?assertEqual(
        {[ok], [{error, exists} || _ <- lists:seq(1, ?config(n, C) - 1)]},
        lists:partition(
            fun
                (ok)         -> true;
                ({error, _}) -> false
            end,
            Results
        )
    ).

%%

-type category() :: atom().

-spec handle_beat
    (consuela_client:beat(), {client, category()}) -> ok;
    (consuela_session_keeper:beat(), {keeper, category()}) -> ok;
    (consuela_zombie_reaper:beat(), {reaper, category()}) -> ok;
    (consuela_registry_server:beat(), {registry, category()}) -> ok
.

handle_beat(Beat, {Producer, Category}) ->
    ct:pal(Category, "[~p] ~p", [Producer, Beat]);
handle_beat(_Beat, _) ->
    ok.
