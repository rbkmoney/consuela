%%%
%%% Top level supervisor

-module(consuela_sup).

%% Environment opts

-type opts() :: [
    {nodename  , consuela_session:nodename()}       |
    {namespace , consuela_registry:namespace()}     |
    {consul    , consul_opts()}                     |
    {session   , session_opts()}                    |
    {keeper    , consuela_session_keeper:opts()}    |
    {registry  , consuela_registry:opts()}
].

-type consul_opts() :: #{
    url  => consuela_client:url(), % <<"http://{nodename}:8500">> by default
    opts => consuela_client:opts() % #{} by default
}.

-type session_opts() :: #{
    name => consuela_session:name(), % '{namespace}' by default
    ttl  => consuela_session:ttl()   % 30 by default
}.

-export_type([opts/0]).

%% API

-export([start_link/0]).

%%

-spec start_link() ->
    {ok, pid()} | {error, _Reason}.

start_link() ->
    Nodename = req_env(nodename),
    Namespace = req_env(namespace),
    Client = mk_consul_client(Nodename, get_env(consul, #{})),
    SessionOpts = mk_session_params(Namespace, Nodename, get_env(session, #{})),
    KeeperOpts = get_env(keeper, #{}),
    RegistryOpts = get_env(registry, #{}),
    consuela_registry_sup:start_link(Namespace, Client, SessionOpts, KeeperOpts, RegistryOpts).

%%

-spec mk_consul_client(inet:hostname(), consul_opts()) ->
    consuela_client:t().

mk_consul_client(Nodename, Opts) ->
    Url = mk_consul_client_url(Nodename, maps:get(url, Opts, undefined)),
    consuela_client:new(Url, Opts).

mk_consul_client_url(_Nodename, Url) when Url /= undefined ->
    Url;
mk_consul_client_url(Nodename, undefined) ->
    genlib:format("http://~s:8500", [Nodename]).

-spec mk_session_params(consuela_registry:namespace(), consuela_session:nodename(), session_opts()) ->
    consuela_registry_sup:session_opts().

mk_session_params(Namespace, Nodename, Opts) ->
    #{
        name => maps:get(name, Opts, Namespace),
        node => Nodename,
        ttl  => maps:get(ttl, Opts, 30)
    }.

%%

req_env(Key) ->
    case application:get_env(consuela, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            exit({missing_required_env, Key})
    end.

get_env(Key, Default) ->
    genlib_app:env(consuela, Key, Default).
