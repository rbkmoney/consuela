%%%
%%% Presence session
%%%
%%% A server which keeps _presence_ in the Consul catalog by registering a service on startup and
%%% deregistering on shutdown.

-module(consuela_presence_session).

%%

-type name()    :: consuela_health:service_name().
-type client()  :: consuela_client:t().

-type seconds() :: non_neg_integer().

-type service() :: #{
    id       := consuela_health:service_id(),
    name     := name(),
    address  := inet:ip_address(),
    tags     => [consuela_health:tag()]
}.

-type opts() :: #{
    interval => seconds(), % 5 by default
    pulse    => {module(), _PulseOpts}
}.

-export([start_link/4]).
-export([get_check_id/1]).

-export_type([name/0]).
-export_type([opts/0]).

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
    {{presence, name()}, started | {stopped, _Reason}} |
    {unexpected, {{call, from()} | cast | info, _Msg}}.

-export_type([beat/0]).

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

%% Only alphanumeric sequences with dashes and underscores allowed for metadata
%% keys.
%% See: https://github.com/hashicorp/consul/blob/v1.5.3/agent/structs/structs.go#L104

-define(MD_NODENAME, <<"consuela-erlang-nodename">>).

%%

-spec start_link(service(), consuela_presence_server:ref(), client(), opts()) ->
    {ok, pid()} | {error, _}.

start_link(Service = #{name := Name}, ServerRef, Client, Opts) ->
    St = mk_state(Service, ServerRef, Client, Opts),
    gen_server:start_link({local, mk_local_ref(Name)}, ?MODULE, St, []).

-spec get_check_id(name()) ->
    consuela_health:check_id().

get_check_id(Name) ->
    gen_server:call(mk_local_ref(Name), get_check_id).

-spec mk_local_ref(name()) ->
    atom().

mk_local_ref(Name) ->
    erlang:binary_to_atom(<<"$consuela_presence_session/", Name/binary>>, latin1).

%%

-type st() :: #{
    id       := consuela_health:service_id(),
    name     := name(),
    address  := inet:ip_address(),
    server   := consuela_health:endpoint(),
    tags     => [consuela_health:tag()],
    interval := non_neg_integer(),
    client   := client(),
    pulse    := {module(), _PulseOpts}
}.

-type from() :: {pid(), reference()}.

-spec mk_state(service(), consuela_presence_server:ref(), client(), opts()) ->
    st().

mk_state(Service, ServerRef, Client, Opts) ->
    {ok, Endpoint} = consuela_presence_server:get_endpoint(ServerRef),
    Service#{
        server   => Endpoint,
        interval => maps:get(interval, Opts, 5),
        client   => Client,
        pulse    => maps:get(pulse, Opts, {?MODULE, []})
    }.

-spec init(st()) ->
    {ok, st(), hibernate}.

init(St = #{name := Name}) ->
    _ = erlang:process_flag(trap_exit, true),
    _ = register_service(St),
    _ = beat({{presence, Name}, started}, St),
    {ok, St, hibernate}.

-spec handle_call(_Call, from(), st()) ->
    {reply, _, st(), hibernate} | {noreply, st(), hibernate}.

handle_call(get_check_id, _From, St = #{id := ServiceID}) ->
    {reply, mk_check_name(ServiceID), St, hibernate};
handle_call(Call, From, St) ->
    _ = beat({unexpected, {{call, From}, Call}}, St),
    {noreply, St, hibernate}.

-spec handle_cast(_Cast, st()) ->
    {noreply, st(), hibernate}.

handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St, hibernate}.

-spec handle_info(_Info, st()) ->
    {noreply, st(), hibernate}.

handle_info(Info, St) ->
    _ = beat({unexpected, {info, Info}}, St),
    {noreply, St, hibernate}.

-spec terminate(_Reason, st()) ->
    ok.

terminate(Reason, St = #{name := Name}) ->
    _ = deregister_service(St),
    _ = beat({{presence, Name}, {stopped, Reason}}, St),
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.

register_service(St = #{
    id := ServiceID,
    name := Name,
    address := Address,
    server := {_, Port},
    interval := Interval,
    client := Client
}) ->
    ServiceParams = #{
        id       => ServiceID,
        name     => Name,
        endpoint => {Address, 0},
        tags     => maps:get(tags, St, []),
        metadata => #{
            ?MD_NODENAME => encode_node(erlang:node())
        },
        checks   => [
            #{
                id      => mk_check_name(ServiceID),
                name    => mk_check_name(Name),
                type    => {tcp, {Address, Port}, Interval},
                initial => passing % So we would be able to start discovery right away
                % TODO
                % https://www.consul.io/api/agent/check.html#deregistercriticalserviceafter ?
            }
        ]
    },
    ok = consuela_health:register(ServiceParams, Client),
    ok.

deregister_service(#{id := ServiceID, client := Client}) ->
    ok = consuela_health:deregister(ServiceID, Client),
    ok.

mk_check_name(Name) ->
    <<Name/binary, ":presence:tcp">>.

encode_node(V) ->
    erlang:atom_to_binary(V, latin1).

decode_node(V) ->
    % TODO
    % Potentially unsafe.
    erlang:binary_to_atom(V, latin1).

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
