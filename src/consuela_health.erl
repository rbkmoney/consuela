%%%
%%% Consul healthchecks

-module(consuela_health).

%%

-type nodename()    :: inet:hostname().
-type servicename() :: binary().
-type tag()         :: binary().
-type status()      :: passing | warning | critical.
-type metadata()    :: #{binary() => binary()}.
-type indexes()     :: #{create | modify => integer()}.

-type uuid()     :: binary().

-type node_() :: #{
    id       := uuid(),
    name     := nodename(),
    address  := inet:ip_address(),
    metadata => metadata(),
    indexes  => indexes()
}.

-type service() :: #{
    id       := binary(),
    name     := servicename(),
    tags     := [tag()],
    address  := inet:ip_address(),
    port     := inet:port_number(),
    metadata => metadata(),
    indexes  => indexes()
}.

-type check() :: #{
    id       := binary(),
    name     := binary(),
    status   := status(),
    indexes  => indexes()
}.

-type t() :: #{
    node     := node_(),
    service  := service(),
    checks   := [check()]
}.

-export_type([servicename/0]).
-export_type([nodename/0]).
-export_type([tag/0]).
-export_type([t/0]).

-export([get/4]).

%%

-spec get(servicename(), [tag()], boolean(), consuela_client:t()) ->
    {ok, [t()]}.

get(ServiceName, Tags, Passing, Client) ->
    Resource = {
        [<<"/v1/health/service/">>, encode_servicename(ServiceName)],
        encode_tags(Tags, encode_passing(Passing, []))
    },
    case consuela_client:request(get, Resource, Client) of
        {ok, Vs} when is_list(Vs) ->
            {ok, [decode_health(V) || V <- Vs]};
        {error, Reason} ->
            erlang:error(Reason)
    end.

%%

encode_tags(V, Q) ->
    lists:foldl(fun encode_tag/2, Q, V).

encode_tag(V, Q) ->
    [{<<"tag">>, encode_binary(V)} | Q].

encode_passing(true, Q) ->
    [{<<"passing">>, <<"true">>} | Q];
encode_passing(false, Q) ->
    Q.

encode_servicename(V) ->
    encode_binary(V).

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
        address  => decode_address(Address),
        ports    => decode_port(Port),
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
