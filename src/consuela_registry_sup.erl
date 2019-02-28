%%%
%%% Registry supervisor
%%%
%%% NOTES
%%%
%%% Supervisor designed with an assumptions that we have no chance for failure. We must stop as soon as we
%%% lose either session keeper (since then we have no right to progress) or registry (since we do not know
%%% what state he was in). If we start consuela app as part of a production node, this leads to an expected
%%% behaviour, which is: _as soon as we lose keeper or registry the node goes down_.
%%%

-module(consuela_registry_sup).

%%

-type opts() :: #{
    nodename  := consuela_session:nodename(),
    namespace := consuela_registry:namespace(),
    consul    => consul_opts(),
    session   => session_opts(),
    keeper    => consuela_session_keeper:opts(), % #{} by default
    reaper    => consuela_zombie_reaper:opts(),  % #{} by default
    registry  => consuela_registry_server:opts() % #{} by default
}.

-type consul_opts() :: #{
    url  => consuela_client:url(), % <<"http://{nodename}:8500">> by default
    opts => consuela_client:opts() % #{} by default
}.

-type session_opts() :: #{
    name => consuela_session:name(), % '{namespace}' by default
    ttl  => consuela_session:ttl()   % 20 by default
}.

-export_type([opts/0]).

-export([start_link/2]).
-export([stop/1]).

%% Supervisor

-behaviour(supervisor).
-export([init/1]).

%%

-spec start_link(consuela_registry_server:ref(), opts()) ->
    {ok, pid()} | {error, _Reason}.

start_link(Ref, Opts) ->
    Nodename     = maps:get(nodename, Opts),
    Namespace    = maps:get(namespace, Opts),
    Client       = mk_consul_client(Nodename, maps:get(consul, Opts, #{})),
    SessionOpts  = mk_session_params(Namespace, Nodename, maps:get(session, Opts, #{})),
    KeeperOpts   = maps:get(keeper, Opts, #{}),
    ReaperOpts   = maps:get(reaper, Opts, #{}),
    RegistryOpts = maps:get(registry, Opts, #{}),
    Session      = mk_session(SessionOpts, Client),
    Registry     = consuela_registry:new(Namespace, Session, Client),
    {ok, Pid} = supervisor:start_link(?MODULE, []),
    {ok, _KeeperPid} = supervisor:start_child(
        Pid,
        #{
            id    => {Ref, keeper},
            start => {consuela_session_keeper, start_link, [Session, Client, KeeperOpts]}
        }
    ),
    {ok, ReaperPid} = supervisor:start_child(
        Pid,
        #{
            id    => {Ref, reaper},
            start => {consuela_zombie_reaper, start_link, [Registry, ReaperOpts]}
        }
    ),
    {ok, _RegistryPid} = supervisor:start_child(
        Pid,
        #{
            id    => {Ref, registry},
            start => {consuela_registry_server, start_link, [Ref, Registry, ReaperPid, RegistryOpts]},
            timeout => infinity
        }
    ),
    {ok, Pid}.

-spec mk_consul_client(inet:hostname(), consul_opts()) ->
    consuela_client:t().

mk_consul_client(Nodename, Opts) ->
    Url = mk_consul_client_url(Nodename, maps:get(url, Opts, undefined)),
    consuela_client:new(Url, maps:get(opts, Opts, #{})).

mk_consul_client_url(_Nodename, Url) when Url /= undefined ->
    Url;
mk_consul_client_url(Nodename, undefined) ->
    genlib:format("http://~s:8500", [Nodename]).

-type session_params() :: #{
    name := consuela_session:name(),
    node := consuela_session:nodename(),
    ttl  := consuela_session:ttl()
}.

-spec mk_session_params(consuela_registry:namespace(), consuela_session:nodename(), session_opts()) ->
    session_params().

mk_session_params(Namespace, Nodename, Opts) ->
    #{
        name => maps:get(name, Opts, Namespace),
        node => Nodename,
        ttl  => maps:get(ttl, Opts, 20)
    }.

mk_session(#{name := Name, node := Node, ttl := TTL}, Client) ->
    {ok, SessionID} = consuela_session:create(Name, Node, TTL, delete, Client),
    {ok, Session} = consuela_session:get(SessionID, Client),
    Session.

-spec stop(pid()) ->
    ok.

stop(Pid) ->
    proc_lib:stop(Pid).

%%

-spec init(nil()) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

init([]) ->
    {ok, {
        #{strategy => one_for_all, intensity => 0, period => 1},
        []
    }}.
