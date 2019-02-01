%%%
%%% Consul session

-module(consuela_session).

%%

-type id()       :: uuid().
-type name()     :: binary().
-type nodename() :: inet:hostname().
-type ttl()      :: non_neg_integer().
-type behavior() :: release | delete.
-type indexes()  :: #{create | modify => integer()}.

-type uuid()     :: binary().

-type t() :: #{
    id       := id(),
    name     := name(),
    node     := nodename(),
    ttl      := ttl(),
    behavior := behavior(),
    indexes  := indexes()
}.

-export_type([name/0]).
-export_type([nodename/0]).
-export_type([ttl/0]).
-export_type([behavior/0]).
-export_type([t/0]).

-export([create/4]).
-export([create/5]).
-export([destroy/2]).
-export([get/2]).
-export([renew/2]).

%%

-spec create(name(), nodename(), ttl(), consuela_client:t()) ->
    {ok, id()}.

-spec create(name(), nodename(), ttl(), behavior(), consuela_client:t()) ->
    {ok, id()}.

-spec get(id(), consuela_client:t()) ->
    {ok, t()} | {error, notfound}.

-spec destroy(id(), consuela_client:t()) ->
    ok.

-spec renew(id(), consuela_client:t()) ->
    {ok, t()}.

create(Name, Node, TTL, Client) ->
    create(Name, Node, TTL, release, Client).

create(Name, Node, TTL, Behavior, Client) ->
    Resource = <<"/v1/session/create">>,
    Content = encode_params({Name, Node, TTL, Behavior}),
    case consuela_client:request(put, Resource, Content, Client) of
        {ok, SessionID} ->
            {ok, decode_session_id(SessionID)}
    end.

get(ID, Client) ->
    Resource = [<<"/v1/session/info/">>, encode_id(ID)],
    case consuela_client:request(get, Resource, Client) of
        {ok, [Session]} ->
            {ok, decode_session(Session)};
        {ok, []} ->
            {error, notfound}
    end.

destroy(ID, Client) ->
    Resource = [<<"/v1/session/destroy/">>, encode_id(ID)],
    case consuela_client:request(delete, Resource, Client) of
        {ok, true} ->
            ok
    end.

renew(ID, Client) ->
    Resource = [<<"/v1/session/renew/">>, encode_id(ID)],
    case consuela_client:request(put, Resource, Client) of
        {ok, [Session]} ->
            {ok, decode_session(Session)}
    end.    

%%

encode_params({Name, Node, TTL, Behavior}) ->
    #{
        <<"Name">>     => encode_name(Name),
        <<"Node">>     => encode_nodename(Node),
        <<"TTL">>      => encode_ttl(TTL),
        <<"Behavior">> => encode_behavior(Behavior)
    }.

encode_id(V) ->
    encode_binary(V).

encode_name(V) ->
    encode_binary(V).

encode_nodename(V) when is_atom(V) ->
    encode_string(erlang:atom_to_list(V));
encode_nodename(V) when is_list(V) ->
    encode_string(V).

encode_ttl(V) ->
    encode_duration('s', V).

encode_behavior(release) ->
    <<"release">>;
encode_behavior(delete) ->
    <<"delete">>.

encode_duration(U, V) when is_integer(V), V >= 0 ->
    consuela_duration:format(V, U).

encode_string(V) when is_list(V) ->
    case unicode:characters_to_binary(V) of B when is_binary(B) -> B end.

encode_binary(V) when is_binary(V) ->
    V.

%%

decode_session_id(#{<<"ID">> := ID}) ->
    decode_id(ID).

decode_session(#{
    <<"ID">>          := ID,
    <<"Name">>        := Name,
    <<"Node">>        := Node,
    <<"LockDelay">>   := _TODO,
    <<"Behavior">>    := Behavior,
    <<"TTL">>         := TTL,
    <<"CreateIndex">> := CreateIndex,
    <<"ModifyIndex">> := ModifyIndex
    % TODO
    % <<"Checks">>    := Checks,
}) ->
    #{
        id       => decode_id(ID),
        name     => decode_name(Name),
        node     => decode_nodename(Node),
        behavior => decode_behavior(Behavior),
        ttl      => decode_ttl(TTL),
        indexes  => #{
            create => decode_index(CreateIndex),
            modify => decode_index(ModifyIndex)
        }
    }.

decode_id(V) ->
    decode_binary(V).

decode_name(V) ->
    decode_binary(V).

decode_nodename(V) ->
    decode_string(V).

decode_ttl(V) ->
    decode_duration('s', V).

decode_behavior(<<"release">>) ->
    release;
decode_behavior(<<"delete">>) ->
    delete.

decode_index(V) ->
    decode_integer(V).

decode_duration(U, V) ->
    consuela_duration:parse(V) div consuela_duration:factor(U).

decode_string(V) when is_binary(V) ->
    case unicode:characters_to_list(V) of S when is_list(S) -> S end.

decode_integer(V) when is_integer(V) ->
    V.

decode_binary(V) when is_binary(V) ->
    V.
