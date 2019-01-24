%%%
%%% Simplistic Consul HTTP API client.

-module(consuela_client).

%% api

-type datacenter() :: iodata().
-type token()      :: iodata().
-type url()        :: iodata().
-type query()      :: [{binary(), binary() | true}]. % cow_qs:qs_vals()
-type headers()    :: [{binary(), {binary(), list({binary(), binary()} | binary())}}].

-opaque t() :: #{
    url     := binary(),
    query   := query(),
    headers := headers(),
    opts    := list(_TODO)
}.

-export_type([datacenter/0]).
-export_type([token/0]).
-export_type([t/0]).

-export([new/2]).
-export([request/3]).
-export([request/4]).

%% pulse

-type beat() ::
    {request, {method(), url(), headers(), binary()}} |
    {result, {ok, 100..599, headers(), binary()} | {error, term()}}.

-callback handle_beat(beat(), _Opts) ->
    _.

-export([handle_beat/2]).

%% api

-define(MAX_RESPONSE_SIZE, 65536).

-type opts() :: #{
    datacenter     => datacenter(),
    acl            => token(),
    transport_opts => #{
        pool            => _Name,
        max_connections => non_neg_integer(),
        connect_timeout => non_neg_integer(),
        recv_timeout    => non_neg_integer(),
        ssl_options     => [ssl:ssl_option()]
    },
    pulse          => {module(), _PulseOpts}
}.

-spec new(url(), opts()) ->
    t().

new(Url, Opts) ->
    maps:fold(
        fun
            (datacenter, V, C = #{query := Q0}) ->
                C#{query := [{<<"dc">>, to_binary(V)} | Q0]};
            (acl, V, C = #{headers := Hs0}) ->
                C#{headers := [{<<"X-Consul-Token">>, to_binary(V)} | Hs0]};
            (transport_opts, V = #{}, C = #{opts := Opts0}) ->
                C#{opts := mk_transport_opts(V, Opts0)};
            (pulse, V = {Module, _Opts}, C = #{}) when is_atom(Module) ->
                C#{pulse := V}
        end,
        #{
            url     => to_binary(Url),
            query   => [],
            headers => [],
            opts    => [with_body, {max_body, ?MAX_RESPONSE_SIZE}],
            pulse   => {?MODULE, []}
        },
        Opts
    ).

mk_transport_opts(Opts, TransOpts0) ->
    maps:fold(
        fun
            (pool, V, TransOpts) ->
                [{pool, V} | TransOpts];
            (ssl_options, V, TransOpts) when is_list(V) ->
                [{ssl_options, V} | TransOpts];
            (Opt, V, TransOpts) when
                Opt == max_connections;
                Opt == connect_timeout;
                Opt == recv_timeout,
                is_integer(V), V > 0
            ->
                [{Opt, V} | TransOpts]
        end,
        TransOpts0,
        Opts
    ).

%%

-type method()   :: get | post | put | delete.
-type resource() :: iodata().
-type content()  :: jsx:json_term() | {raw, iodata()} | undefined.

-type reason() ::
    {client_error, {400..499, binary()}} | % TODO Less HTTP details? E.g. `notfound`, `forbidden`, etc.
    {server_error, {500..599, binary()}} |
    {transport_error, term()}.

-spec request(method(), resource(), t()) ->
    {ok, jsx:json_term()} | {error, reason()}.

request(Method, Resource, C) ->
    request(Method, Resource, undefined, C).

-spec request(method(), resource(), content(), t()) ->
    {ok, jsx:json_term()} | {error, reason()}.

request(Method, Resource, Content, C) ->
    Url = mk_request_url(Resource, C),
    Request = {Method, Url, mk_headers(Content, C), mk_body(Content)},
    case issue_request(Request, C) of
        {ok, Status, _Headers, RespBody} when Status >= 200, Status < 300 ->
            decode_response(RespBody);
        {ok, Status, _Headers, RespBody} when Status >= 400, Status < 500 ->
            {error, {client_error, Status, RespBody}};
        {ok, Status, _Headers, RespBody} when Status >= 500, Status < 600 ->
            {error, {server_error, {Status, RespBody}}};
        {error, Reason} ->
            {error, {transport_error, Reason}}
    end.

decode_response(<<>>) ->
    ok;
decode_response(RespBody) ->
    {ok, jsx:decode(RespBody, [return_maps])}.

mk_request_url(Resource, #{url := Url, query := Query}) ->
    Qs = cow_qs:qs(Query),
    Path = to_binary(Resource),
    <<Url/binary, Path/binary, "?", Qs/binary>>.

mk_headers(undefined, #{headers := Hs}) ->
    Hs;
mk_headers({raw, _}, #{headers := Hs}) ->
    Hs;
mk_headers(_Json, #{headers := Hs}) ->
    [{<<"Content-Type">>, {<<"applicaton/json">>, [{<<"charset">>, <<"utf-8">>}]}} | Hs].

mk_body(undefined) ->
    <<>>;
mk_body({raw, Bytes}) ->
    Bytes;
mk_body(JsonTerm) ->
    jsx:encode(JsonTerm).

issue_request(Request = {Method, Url, Headers, Body}, C = #{opts := TransOpts}) ->
    _ = beat({request, Request}, C),
    Result = hackney:request(Method, Url, Headers, Body, TransOpts),
    _ = beat({result, Result}, C),
    Result.

%%

-spec beat(beat(), t()) ->
    _.

beat(Beat, #{pulse := {Module, Opts}}) ->
    % TODO handle errors?
    Module:handle_beat(Beat, Opts).

to_binary(V) ->
    erlang:iolist_to_binary(V).

%% pulse

-include_lib("kernel/include/logger.hrl").

-spec handle_beat(beat(), [trace]) ->
    ok.

handle_beat(Beat, [trace]) ->
    ?LOG_DEBUG("[~p] ~p", [?MODULE, Beat]);
handle_beat(_Beat, _) ->
    ok.
