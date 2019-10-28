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
    opts    := list(_TODO),
    pulse   := {module(), _PulseOpts}
}.

-export_type([url/0]).
-export_type([datacenter/0]).
-export_type([token/0]).
-export_type([t/0]).

-export([new/2]).
-export([request/3]).
-export([request/4]).

%% pulse

-type beat() ::
    {request, {method(), binary(), headers(), binary()}} |
    {result, {ok, 100..599, headers(), binary()} | {error, term()}}.

-callback handle_beat(beat(), _Opts) ->
    _.

-export_type([beat/0]).

-export([handle_beat/2]).

%% api

-type opts() :: #{
    datacenter     => datacenter(),
    acl            => token(),
    transport_opts => #{
        pool              => _Name,
        max_connections   => non_neg_integer(),
        max_response_size => pos_integer(),     % bytes
        connect_timeout   => non_neg_integer(), % milliseconds
        recv_timeout      => non_neg_integer(), % milliseconds
        ssl_options       => [ssl:tls_client_option()]
    },
    pulse          => {module(), _PulseOpts}
}.

-export_type([opts/0]).

-spec new(url(), opts()) ->
    t().

new(Url, Opts) ->
    maps:fold(
        fun
            (datacenter, V, C = #{query := Q0}) ->
                C#{query := [{<<"dc">>, to_binary(V)} | Q0]};
            (acl, {file, Path}, C = #{headers := Hs0}) ->
                % TODO error handling
                {ok, V} = file:read_file(Path),
                C#{headers := [{<<"X-Consul-Token">>, string:trim(V)} | Hs0]};
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
            opts    => [with_body],
            pulse   => {?MODULE, []}
        },
        Opts
    ).

mk_transport_opts(Opts, TransOpts0) ->
    maps:fold(
        fun
            (pool, V, TransOpts) ->
                set_opt(pool, V, TransOpts);
            (ssl_options, V, TransOpts) when is_list(V) ->
                set_opt(ssl_options, V, TransOpts);
            (max_response_size, V, TransOpts) when is_integer(V), V > 0 ->
                set_opt(max_body, V, TransOpts);
            (Opt, V, TransOpts) when
                Opt == max_connections;
                Opt == connect_timeout;
                Opt == recv_timeout,
                is_integer(V), V > 0
            ->
                set_opt(Opt, V, TransOpts)
        end,
        set_opt(max_body, 65536, TransOpts0),
        Opts
    ).

set_opt(Name, V, Opts) ->
    lists:keystore(Name, 1, Opts, {Name, V}).

%%

-type method()   :: get | post | put | delete.
-type resource() :: iodata() | {iodata(), query()}.
-type content()  :: jsx:json_term() | {raw, iodata()} | undefined.

-type reason() ::
    unauthorized                   |
    notfound                       |
    {error_class(), error_cause()} .

-type error_class() :: failed | unknown.
-type error_cause() ::
    {server_error, {500..599, binary()}} |
    {transport_error, term()}.

-spec request(method(), resource(), t()) ->
    {ok, jsx:json_term() | undefined} | {error, reason()}.

request(Method, Resource, C) ->
    request(Method, Resource, undefined, C).

-spec request(method(), resource(), content(), t()) ->
    {ok, jsx:json_term() | undefined} | {error, reason()}.

request(Method, Resource, Content, C) ->
    Url = mk_request_url(Resource, C),
    Request = {Method, Url, mk_headers(Content, C), mk_body(Content)},
    case issue_request(Request, C) of
        {ok, Status, _Headers, RespBody} when Status >= 200, Status < 300 ->
            decode_response(RespBody);
        {ok, Status, _Headers, _RespBody} when Status >= 400, Status < 500 ->
            {error, get_client_error(Status)};
        {ok, Status, _Headers, RespBody} when Status >= 500, Status < 600 ->
            {error, {
                get_server_error_class(Status),
                {server_error, {Status, RespBody}}
            }};
        {error, Reason} ->
            {error, {
                get_transport_error_class(Reason),
                {transport_error, Reason}}
            }
    end.

decode_response(<<>>) ->
    {ok, undefined};
decode_response(RespBody) ->
    {ok, jsx:decode(RespBody, [return_maps])}.

mk_request_url({Resource, Query}, #{url := Url, query := Query0}) ->
    Qs = cow_qs:qs(Query ++ Query0),
    Path = to_binary(Resource),
    <<Url/binary, Path/binary, "?", Qs/binary>>;
mk_request_url(Resource, Client) ->
    mk_request_url({Resource, []}, Client).

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

get_client_error(401) ->
    unauthorized;
get_client_error(404) ->
    notfound.

get_server_error_class(Code) when
    Code == 501 orelse
    Code == 503
->
    failed;
get_server_error_class(_) ->
    unknown.

get_transport_error_class(Reason) when
    Reason == nxdomain orelse
    Reason == enetdown orelse
    Reason == enetunreach orelse
    Reason == econnrefused orelse
    Reason == connect_timeout
->
    failed;
get_transport_error_class(_) ->
    unknown.

to_binary(V) ->
    erlang:iolist_to_binary(V).

%%

-spec beat(beat(), t()) ->
    _.

beat(Beat, #{pulse := {Module, Opts}}) ->
    % TODO handle errors?
    Module:handle_beat(Beat, Opts).

%% pulse

-spec handle_beat(beat(), [trace]) ->
    ok.

handle_beat(Beat, [trace]) ->
    logger:debug("[~p] ~p", [?MODULE, Beat]);
handle_beat(_Beat, _) ->
    ok.
