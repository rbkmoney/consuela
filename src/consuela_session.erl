%%%
%%% Consul session

-module(consuela_session).

%%

-type id() :: uuid().
-type name() :: binary().
-type nodename() :: inet:hostname().
-type ttl() :: seconds().
-type check() :: consuela_health:check_id().
-type delay() :: seconds().
-type behavior() :: release | delete.
-type indexes() :: #{create | modify => integer()}.

-type uuid() :: binary().
-type seconds() :: non_neg_integer().

-type t() :: #{
    id := id(),
    name := name(),
    node := nodename(),
    checks := [check()],
    ttl := ttl(),
    lock_delay := delay(),
    behavior := behavior(),
    indexes := indexes()
}.

-export_type([id/0]).
-export_type([name/0]).
-export_type([nodename/0]).
-export_type([ttl/0]).
-export_type([delay/0]).
-export_type([behavior/0]).
-export_type([t/0]).

-export([create/4]).
-export([create/5]).
-export([destroy/2]).
-export([get/2]).
-export([renew/2]).

%%

-type opts() :: #{
    % [] by default
    checks => [check()],
    lock_delay => delay(),
    behavior => behavior()
}.

-spec create(name(), nodename(), ttl(), consuela_client:t()) -> {ok, id()}.

-spec create(name(), nodename(), ttl(), opts(), consuela_client:t()) -> {ok, id()}.

-spec get(id(), consuela_client:t()) -> {ok, t()} | {error, notfound}.

-spec destroy(id(), consuela_client:t()) -> ok.

-spec renew(id(), consuela_client:t()) -> {ok, t()}.

create(Name, Node, TTL, Client) ->
    create(Name, Node, TTL, #{}, Client).

create(Name, Node, TTL, Opts, Client) ->
    Resource = <<"/v1/session/create">>,
    Content = encode_params(mk_params(Name, Node, TTL, Opts)),
    case consuela_client:request(put, Resource, Content, Client) of
        {ok, SessionID} ->
            {ok, decode_session_id(SessionID)};
        {error, Reason} ->
            erlang:error(Reason)
    end.

mk_params(Name, Node, TTL, Opts) ->
    % NOTE
    % We deliberately override Consul defaults here despite strong recommendations against such measures.
    % This is because letting serfHealth decide if the node is dead proved too unreliable during stress
    % testing. We were able to trigger session invalidation when consuela app was alive and stable, session
    % had 10 seconds more to live, even the node was on the majority side of a cluster.
    Params = #{
        name => Name,
        node => Node,
        ttl => TTL,
        checks => []
    },
    maps:merge(Params, Opts).

get(ID, Client) ->
    Resource = [<<"/v1/session/info/">>, encode_id(ID)],
    case consuela_client:request(get, Resource, Client) of
        {ok, [Session]} ->
            {ok, decode_session(Session)};
        {ok, []} ->
            {error, notfound};
        {error, Reason} ->
            erlang:error(Reason)
    end.

destroy(ID, Client) ->
    Resource = [<<"/v1/session/destroy/">>, encode_id(ID)],
    case consuela_client:request(put, Resource, Client) of
        {ok, true} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason)
    end.

renew(ID, Client) ->
    Resource = [<<"/v1/session/renew/">>, encode_id(ID)],
    case consuela_client:request(put, Resource, Client) of
        {ok, [Session]} ->
            {ok, decode_session(Session)};
        {error, Reason} ->
            erlang:error(Reason)
    end.

%%

encode_params(Params = #{name := Name, node := Node, ttl := TTL}) ->
    maps:fold(
        fun
            (behavior, V, R) -> R#{<<"Behavior">> => encode_behavior(V)};
            (lock_delay, V, R) -> R#{<<"LockDelay">> => encode_seconds(V)};
            (checks, V, R) -> R#{<<"Checks">> => encode_checks(V)};
            (_, _, R) -> R
        end,
        #{
            <<"Name">> => encode_name(Name),
            <<"Node">> => encode_nodename(Node),
            <<"TTL">> => encode_seconds(TTL)
        },
        Params
    ).

encode_id(V) ->
    encode_binary(V).

encode_name(V) ->
    encode_binary(V).

encode_nodename(V) when is_atom(V) ->
    encode_string(erlang:atom_to_list(V));
encode_nodename(V) when is_list(V) ->
    encode_string(V).

encode_checks(V) when is_list(V) ->
    [encode_id(E) || E <- V].

encode_seconds(V) ->
    encode_duration('s', V).

encode_behavior(release) ->
    <<"release">>;
encode_behavior(delete) ->
    <<"delete">>.

encode_duration(U, V) when is_integer(V), V >= 0 ->
    consuela_duration:format(V, U).

encode_string(V) when is_list(V) ->
    case unicode:characters_to_binary(V) of
        B when is_binary(B) -> B
    end.

encode_binary(V) when is_binary(V) ->
    V.

%%

decode_session_id(#{<<"ID">> := ID}) ->
    decode_id(ID).

decode_session(#{
    <<"ID">> := ID,
    <<"Name">> := Name,
    <<"Node">> := Node,
    <<"Behavior">> := Behavior,
    <<"Checks">> := Checks,
    <<"TTL">> := TTL,
    <<"LockDelay">> := LockDelay,
    <<"CreateIndex">> := CreateIndex,
    <<"ModifyIndex">> := ModifyIndex
}) ->
    #{
        id => decode_id(ID),
        name => decode_name(Name),
        node => decode_nodename(Node),
        behavior => decode_behavior(Behavior),
        checks => decode_checks(Checks),
        ttl => decode_seconds(TTL),
        lock_delay => decode_seconds(LockDelay),
        indexes => #{
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

decode_checks(V) when is_list(V) ->
    [decode_id(E) || E <- V];
decode_checks(null) ->
    [].

decode_seconds(V) ->
    decode_duration('s', V).

decode_behavior(<<"release">>) ->
    release;
decode_behavior(<<"delete">>) ->
    delete.

decode_index(V) ->
    decode_integer(V).

decode_duration(U, V) when is_integer(V) ->
    V div consuela_duration:factor(U);
decode_duration(U, V) ->
    consuela_duration:parse(V) div consuela_duration:factor(U).

decode_string(V) when is_binary(V) ->
    case unicode:characters_to_list(V) of
        S when is_list(S) -> S
    end.

decode_integer(V) when is_integer(V) ->
    V.

decode_binary(V) when is_binary(V) ->
    V.
