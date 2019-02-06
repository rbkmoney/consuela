%%%
%%% Consul lock

-module(consuela_lock).

%%

-type namespace()   :: binary().
-type id()          :: {namespace(), term()}.
-type value()       :: term().
-type index()       :: integer().
-type session()     :: consuela_session:id().
-type consistency() :: default | consistent | stale.

-type lock() :: #{
    id      := id(),
    value   := value(),
    session => session(),
    indexes := {_Create :: index(), _Modify :: index(), _Lock :: index()}
}.

-export_type([namespace/0]).
-export_type([lock/0]).

-export([hold/4]).
-export([release/2]).
-export([release/3]).
-export([get/2]).
-export([get/3]).

%%

-spec hold(id(), value(), session(), consuela_client:t()) ->
    ok | {error, failed}.

hold(ID, Value, Session, Client) ->
    Resource = {[<<"/v1/kv/">> | encode_id(ID)], [{<<"acquire">>, Session}]},
    case consuela_client:request(put, Resource, {raw, encode_value(Value)}, Client) of
        {ok, true} ->
            ok;
        {ok, false} ->
            {error, failed}
        % TODO
        % handle `{error,{server_error,{500, <<"invalid session...">>}}}`?
        % handle `{error,{server_error,{500, <<"rpc error making call: invalid session...">>}}}`?
    end.

-spec release(lock(), consuela_client:t()) ->
    ok | {error, failed}.

-spec release(id(), session(), consuela_client:t()) ->
    ok | {error, failed}.

release(#{id := ID, session := Session}, Client) ->
    release(ID, Session, Client);
release(#{}, _Client) ->
    erlang:error(badarg).

release(ID, Session, Client) ->
    Resource = {[<<"/v1/kv/">> | encode_id(ID)], [{<<"release">>, Session}]},
    case consuela_client:request(put, Resource, Client) of
        {ok, true} ->
            ok;
        {ok, false} ->
            {error, failed}
    end.

-spec get(id(), consuela_client:t()) ->
    {ok, lock()} | {error, notfound}.

get(ID, Client) ->
    get(ID, default, Client).

-spec get(id(), consistency(), consuela_client:t()) ->
    {ok, lock()} | {error, notfound}.

get(ID = {NS, _}, Consistency, Client) ->
    Resource = {[<<"/v1/kv/">> | encode_id(ID)], [{encode_consistency(Consistency), true}]},
    case consuela_client:request(get, Resource, Client) of
        {ok, [Lock]} ->
            {ok, decode_lock(Lock, NS)};
        {error, notfound} ->
            {error, notfound}
    end.

%%

encode_id({NS, Name}) ->
    [NS, $/, encode_name(Name)].

encode_consistency(default) ->
    [];
encode_consistency(consistent) ->
    [{<<"consistent">>, true}];
encode_consistency(stale) ->
    [{<<"stale">>, true}].

encode_name(V) ->
    encode_base62(encode_term(V)).

encode_value(V) ->
    encode_term(V).

encode_base62(V) ->
    genlib_format:format_int_base(binary:decode_unsigned(V), 62).

encode_term(V) ->
    erlang:term_to_binary(V).

%%

decode_lock(#{
    <<"Key">>         := Name,
    <<"Value">>       := Value,
    <<"Session">>     := Session,
    <<"CreateIndex">> := CreateIndex,
    <<"ModifyIndex">> := ModifyIndex,
    <<"LockIndex">>   := LockIndex
    % TODO
    % <<"Flags">>     := Flags,
}, NS) ->
    genlib_map:compact(#{
        id      => {NS, decode_name(Name)},
        value   => decode_value(Value),
        session => decode_maybe(fun decode_session/1, Session),
        indexes => {decode_index(CreateIndex), decode_index(ModifyIndex), decode_index(LockIndex)}
    }).

decode_session(V) ->
    decode_binary(V).

decode_maybe(_, undefined) ->
    undefined;
decode_maybe(D, V) ->
    D(V).

decode_name(V) ->
    decode_term(decode_base62(V)).

decode_value(V) ->
    decode_term(decode_base64(V)).

decode_index(V) ->
    decode_integer(V).

decode_base62(V) ->
    binary:encode_unsigned(genlib_format:parse_int_base(V, 62)).

decode_base64(V) ->
    base64:decode(V).

decode_term(V) ->
    erlang:binary_to_term(V).

decode_integer(V) when is_integer(V) ->
    V.

decode_binary(V) when is_binary(V) ->
    V.