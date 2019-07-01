%%%
%%% Consul healthchecks

-module(consuela_health).

%%

-type node_name()    :: inet:hostname().
-type service_id()   :: id().
-type service_name() :: binary().
-type check_id()     :: id().
-type check_name()   :: binary().
-type tag()          :: binary().
-type status()       :: passing | warning | critical.
-type endpoint()     :: {inet:ip_address() | undefined, inet:port_number()}.
-type metadata()     :: #{binary() => binary()}.
-type indexes()      :: #{create | modify => integer()}.

-type id()           :: binary().
-type uuid()         :: binary().
-type seconds()      :: non_neg_integer().

-type node_() :: #{
    id       := uuid(),
    name     := node_name(),
    address  := inet:ip_address(),
    metadata => metadata(),
    indexes  => indexes()
}.

-type service() :: #{
    id       := service_id(),
    name     := service_name(),
    tags     := [tag()],
    endpoint := endpoint(),
    metadata => metadata(),
    indexes  => indexes()
}.

-type check() :: #{
    id       := check_id(),
    name     := check_name(),
    status   := status(),
    indexes  => indexes()
}.

-type t() :: #{
    node     := node_(),
    service  := service(),
    checks   := [check()]
}.

-export_type([service_id/0]).
-export_type([service_name/0]).
-export_type([check_id/0]).
-export_type([node_name/0]).
-export_type([tag/0]).
-export_type([t/0]).

-export([get/4]).
-export([register/2]).
-export([deregister/2]).

%%

-spec get(service_name(), [tag()], boolean(), consuela_client:t()) ->
    {ok, [t()]}.

get(ServiceName, Tags, Passing, Client) ->
    Resource = {
        [<<"/v1/health/service/">>, encode_servicename(ServiceName)],
        attach_tags(Tags, attach_passing(Passing, []))
    },
    case consuela_client:request(get, Resource, Client) of
        {ok, Vs} when is_list(Vs) ->
            {ok, [decode_health(V) || V <- Vs]};
        {error, Reason} ->
            erlang:error(Reason)
    end.

-type service_params() :: #{
    name     := service_name(),
    id       => service_id(),
    tags     := [tag()],
    endpoint := endpoint(),
    checks   => [check_params()]
}.

-type check_params() :: #{
    name     := check_name(),
    id       => check_id(),
    type     := {ttl, seconds()}, % TODO | {http, ...}
    initial  => status()
}.

-spec register(service_params(), consuela_client:t()) ->
    ok.

register(ServiceParams, Client) ->
    Resource = <<"/v1/agent/service/register">>,
    Content = encode_service_params(ServiceParams),
    case consuela_client:request(put, Resource, Content, Client) of
        {ok, undefined} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason)
    end.

-spec deregister(service_name() | service_id(), consuela_client:t()) ->
    ok | {error, notfound}.

deregister(ServiceNameOrID, Client) ->
    Resource = [<<"/v1/agent/service/deregister/">>, encode_servicename(ServiceNameOrID)],
    case consuela_client:request(put, Resource, Client) of
        {ok, undefined} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason)
    end.

%%

attach_tags(V, Q) ->
    lists:foldl(fun attach_tag/2, Q, V).

attach_tag(V, Q) ->
    [{<<"tag">>, encode_binary(V)} | Q].

attach_passing(true, Q) ->
    [{<<"passing">>, <<"true">>} | Q];
attach_passing(false, Q) ->
    Q.

%%

encode_service_params(#{
    name     := Name,
    tags     := Tags,
    endpoint := {Address, Port}
} = V) ->
    maps:merge(
        #{
            <<"Name">>    => encode_servicename(Name),
            <<"ID">>      => encode_binary(maps:get(id, V, genlib:to_binary(Name))),
            <<"Tags">>    => encode_tags(Tags),
            <<"Address">> => encode_address(Address),
            <<"Port">>    => encode_port(Port)
        },
        encode_checks(maps:get(checks, V, undefined))
    ).

encode_tags(V) ->
    lists:map(fun encode_tag/1, V).

encode_tag(V) ->
    encode_binary(V).

encode_address(V) ->
    case inet:ntoa(V) of
        R when is_list(R) ->
            encode_string(R);
        {error, einval} ->
            erlang:error(badarg)
    end.

encode_port(V) ->
    encode_integer(V).

encode_checks(undefined) ->
    #{};
encode_checks(V) when is_list(V) ->
    #{<<"Checks">> => lists:map(fun encode_check_params/1, V)}.

encode_check_params(#{name := Name, type := Type} = V) ->
    maps:merge(
        #{
            <<"Name">>   => encode_servicename(Name),
            <<"ID">>     => encode_binary(maps:get(id, V, Name)),
            <<"Status">> => encode_status(maps:get(initial, V, critical))
        },
        encode_check_type(Type)
    ).

encode_check_type({ttl, V}) ->
    #{
        <<"TTL">> => encode_duration('s', V)
    }.

encode_status(passing) ->
    <<"passing">>;
encode_status(warning) ->
    <<"warning">>;
encode_status(critical) ->
    <<"critical">>.

encode_servicename(V) ->
    encode_binary(V).

encode_duration(U, V) when is_integer(V), V >= 0 ->
    consuela_duration:format(V, U).

encode_integer(V) when is_integer(V) ->
    V.

encode_string(V) ->
    encode_binary(unicode:characters_to_binary(V)).

encode_binary(V) when is_binary(V) ->
    V.

%%

decode_health(#{
    <<"Node">>    := Node,
    <<"Service">> := Service,
    <<"Checks">>  := Checks
}) ->
    #{
        node    => decode_node(Node),
        service => decode_service(Service),
        checks  => decode_checks(Checks)
    }.

decode_node(#{
    <<"ID">>          := ID,
    <<"Node">>        := Node,
    <<"Address">>     := Address,
    <<"Meta">>        := Meta,
    <<"CreateIndex">> := CreateIndex,
    <<"ModifyIndex">> := ModifyIndex
    % TODO
    % <<"Datacenter">> := Datacenter,
    % <<"TaggedAddresses">> := TaggedAddresses
}) ->
    decode_meta(Meta, #{
        id       => decode_id(ID),
        name     => decode_nodename(Node),
        address  => decode_address(Address),
        indexes  => #{
            create => decode_index(CreateIndex),
            modify => decode_index(ModifyIndex)
        }
    }).

decode_service(#{
    <<"ID">>          := ID,
    <<"Service">>     := Service,
    <<"Tags">>        := Tags,
    <<"Address">>     := Address,
    <<"Port">>        := Port,
    <<"Meta">>        := Meta,
    <<"CreateIndex">> := CreateIndex,
    <<"ModifyIndex">> := ModifyIndex
    % TODO
    % <<"Weights">> := Weights,
    % <<"Proxy">> := Proxy,
    % <<"Connect">> := Connect
}) ->
    decode_meta(Meta, #{
        id       => decode_id(ID),
        name     => decode_servicename(Service),
        tags     => decode_tags(Tags),
        endpoint => {decode_address(Address), decode_port(Port)},
        indexes  => #{
            create => decode_index(CreateIndex),
            modify => decode_index(ModifyIndex)
        }
    }).

decode_tags(V) ->
    lists:map(fun decode_tag/1, V).

decode_tag(V) ->
    decode_binary(V).

decode_port(V) ->
    decode_integer(V).

decode_checks(V) ->
    lists:map(fun decode_check/1, V).

decode_check(#{
    <<"CheckID">>     := ID,
    <<"Name">>        := Name,
    <<"Status">>      := Status,
    <<"CreateIndex">> := CreateIndex,
    <<"ModifyIndex">> := ModifyIndex
    % TODO
    % <<"Notes">> := Notes,
    % <<"Output">> := Output,
    % <<"ServiceID">> := ServiceID,
    % <<"ServiceName">> := ServiceName,
    % <<"ServiceTags">> := ServiceTags,
    % <<"Definition">> := Definition
}) ->
    #{
        id       => decode_id(ID),
        name     => decode_checkname(Name),
        status   => decode_status(Status),
        indexes  => #{
            create => decode_index(CreateIndex),
            modify => decode_index(ModifyIndex)
        }
    }.

decode_meta(V, Acc) when is_map(V) ->
    Acc#{metadata => V};
decode_meta(null, Acc) ->
    Acc.

decode_id(V) ->
    decode_binary(V).

decode_servicename(V) ->
    decode_binary(V).

decode_checkname(V) ->
    decode_binary(V).

decode_nodename(V) ->
    decode_string(V).

decode_address(<<>>) ->
    undefined;
decode_address(V) ->
    case inet:parse_address(erlang:binary_to_list(V)) of
        {ok, R} ->
            R;
        {error, einval} ->
            erlang:error(badarg)
    end.

decode_status(<<"passing">>) ->
    passing;
decode_status(<<"warning">>) ->
    warning;
decode_status(<<"critical">>) ->
    critical.

decode_index(V) ->
    decode_integer(V).

decode_string(V) when is_binary(V) ->
    case unicode:characters_to_list(V) of S when is_list(S) -> S end.

decode_integer(V) when is_integer(V) ->
    V.

decode_binary(V) when is_binary(V) ->
    V.
