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

-type nodename() :: consuela_session:nodename().
-type namespace() :: consuela_registry:namespace().

-type opts() :: #{
    nodename  := nodename(),
    namespace := namespace(),
    consul    => consul_opts(),
    session   => session_opts(),
    keeper    => consuela_session_keeper:opts(), % #{} by default
    reaper    => consuela_zombie_reaper:opts(),  % #{} by default
    registry  => consuela_registry_server:opts(), % #{} by default
    shutdown  => timeout() % if not specified uses supervisor default of 5000 ms
}.

-type consul_opts() :: #{
    url  => consuela_client:url(), % <<"http://{nodename}:8500">> by default
    opts => consuela_client:opts() % #{} by default
}.

-type session_opts() :: #{
    name       => consuela_session:name(), % '{namespace}' by default
    presence   => consuela_presence_session:name(), % which checks to associate
    ttl        => consuela_session:ttl(),  % 20 by default
    lock_delay => consuela_session:delay() % 10 by default
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
    KeeperOpts   = maps:get(keeper, Opts, #{}),
    ReaperOpts   = maps:get(reaper, Opts, #{}),
    RegistryOpts = maps:get(registry, Opts, #{}),
    Session      = mk_session(Namespace, Nodename, Client, maps:get(session, Opts, #{})),
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
        maps:merge(
            #{
                id    => {Ref, reaper},
                start => {consuela_zombie_reaper, start_link, [Registry, ReaperOpts]}
            },
            maps:with([shutdown], Opts)
        )
    ),
    {ok, _RegistryPid} = supervisor:start_child(
        Pid,
        #{
            id    => {Ref, registry},
            start => {consuela_registry_server, start_link, [Ref, Registry, ReaperPid, RegistryOpts]}
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

-spec mk_session(namespace(), nodename(), consuela_client:t(), session_opts()) ->
    consuela_session:t().

mk_session(Namespace, Node, Client, Opts) ->
    Name = maps:get(name, Opts, Namespace),
    TTL = maps:get(ttl, Opts, 20),
    Opts0 = #{
        behavior   => delete,
        lock_delay => maps:get(lock_delay, Opts, 10)
    },
    Opts1 = case maps:find(presence, Opts) of
        {ok, Presence} ->
            Opts0#{checks => [consuela_presence_session:get_check_id(Presence)]};
        error ->
            Opts0
    end,
    {ok, SessionID} = consuela_session:create(Name, Node, TTL, Opts1, Client),
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
