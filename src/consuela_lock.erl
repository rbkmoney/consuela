%%%
%%% Consul lock

-module(consuela_lock).

%%

-type namespace() :: binary().
-type id() :: {namespace(), term()}.
-type value() :: term().
-type index() :: integer().
-type session() :: consuela_session:id().
-type consistency() :: default | consistent | stale.

-type lock() :: #{
    id := id(),
    value := value(),
    session => session(),
    indexes := {_Create :: index(), _Modify :: index(), _Lock :: index()}
}.

-export_type([namespace/0]).
-export_type([lock/0]).

-export([hold/4]).
-export([release/2]).
-export([delete/2]).
-export([get/2]).
-export([get/3]).

%%

-spec hold(id(), value(), session(), consuela_client:t()) -> ok | {error, failed}.
hold(ID, Value, Session, Client) ->
    Resource = {[<<"/v1/kv/">> | encode_id(ID)], [{<<"acquire">>, encode_session(Session)}]},
    case consuela_client:request(put, Resource, {raw, encode_value(Value)}, Client) of
        {ok, true} ->
            % Lock acquired successfully
            ok;
        {ok, false} ->
            % Either some other session holds a lock or: `Rejecting lock of ... due to lock-delay until ...`
            {error, failed};
        % TODO
        % handle `{error,{server_error,{500, <<"invalid session...">>}}}`?
        % handle `{error,{server_error,{500, <<"rpc error making call: invalid session...">>}}}`?
        {error, Reason} ->
            erlang:error(Reason)
    end.

-spec release(lock(), consuela_client:t()) -> ok | {error, failed}.
release(#{id := ID, session := Session}, Client) ->
    release(ID, Session, Client);
release(#{}, _Client) ->
    erlang:error(badarg).

-spec release(id(), session(), consuela_client:t()) -> ok | {error, failed}.
release(ID, Session, Client) ->
    Resource = {[<<"/v1/kv/">> | encode_id(ID)], [{<<"release">>, encode_session(Session)}]},
    case consuela_client:request(put, Resource, Client) of
        {ok, true} ->
            ok;
        {ok, false} ->
            {error, failed};
        {error, Reason} ->
            erlang:error(Reason)
    end.

-spec delete(lock(), consuela_client:t()) -> ok | {error, stale}.
delete(#{id := ID, indexes := {_, ModifyIndex, _}}, Client) ->
    Resource = {[<<"/v1/kv/">> | encode_id(ID)], encode_cas(ModifyIndex)},
    case consuela_client:request(delete, Resource, Client) of
        {ok, true} ->
            ok;
        {ok, false} ->
            {error, stale};
        {error, Reason} ->
            erlang:error(Reason)
    end.

-spec get(id(), consuela_client:t()) -> {ok, lock()} | {error, notfound}.
get(ID, Client) ->
    get(ID, default, Client).

-spec get(id(), consistency(), consuela_client:t()) -> {ok, lock()} | {error, notfound}.
get(ID, Consistency, Client) ->
    Resource = {[<<"/v1/kv/">> | encode_id(ID)], encode_consistency(Consistency)},
    case consuela_client:request(get, Resource, Client) of
        {ok, [Lock]} ->
            {ok, decode_lock(Lock, ID)};
        {error, notfound} ->
            {error, notfound};
        {error, Reason} ->
            erlang:error(Reason)
    end.

%%

encode_id({NS, Name}) ->
    [encode_namespace(NS), $/, encode_name(Name)].

encode_namespace(V) ->
    cow_uri:urlencode(V).

encode_consistency(default) ->
    [];
encode_consistency(consistent) ->
    [{<<"consistent">>, true}];
encode_consistency(stale) ->
    [{<<"stale">>, true}].

encode_cas(V) ->
    [{<<"cas">>, erlang:integer_to_binary(V)}].

encode_name(V) ->
    encode_base62(encode_term(V)).

encode_value(V) ->
    encode_term(V).

encode_session(V) ->
    encode_binary(V).

encode_base62(V) ->
    genlib_format:format_int_base(binary:decode_unsigned(V), 62).

encode_term(V) ->
    erlang:term_to_binary(V).

encode_binary(V) when is_binary(V) ->
    V.

%%

decode_lock(
    V = #{
        <<"Value">> := Value,
        <<"CreateIndex">> := CreateIndex,
        <<"ModifyIndex">> := ModifyIndex,
        <<"LockIndex">> := LockIndex
        % TODO
        % <<"Flags">>     := Flags,
    },
    ID
) ->
    maps:fold(
        fun
            (<<"Session">>, Session, L) -> L#{session => decode_session(Session)};
            (_, _, L) -> L
        end,
        #{
            id => ID,
            value => decode_nullable(fun decode_value/1, Value),
            indexes => {decode_index(CreateIndex), decode_index(ModifyIndex), decode_index(LockIndex)}
        },
        V
    ).

decode_session(V) ->
    decode_binary(V).

decode_nullable(_, null) ->
    undefined;
decode_nullable(D, V) ->
    D(V).

decode_value(V) ->
    decode_term(decode_base64(V)).

decode_index(V) ->
    decode_integer(V).

decode_base64(V) ->
    base64:decode(V).

decode_term(V) ->
    erlang:binary_to_term(V).

decode_integer(V) when is_integer(V) ->
    V.

decode_binary(V) when is_binary(V) ->
    V.
