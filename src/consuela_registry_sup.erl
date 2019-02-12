%%%
%%% Registry supervisor

-module(consuela_registry_sup).

%%

-export([start_link/5]).

%% Supervisor

-behaviour(supervisor).
-export([init/1]).

%%

-export_type([session_opts/0]).

-type session_opts() :: #{
    name := consuela_session:name(),
    node := consuela_session:nodename(),
    ttl  := consuela_session:ttl()
}.

-spec start_link(
    consuela_registry:namespace(),
    consuela_client:t(),
    session_opts(),
    consuela_session_keeper:opts(),
    consuela_registry:opts()
) ->
    {ok, pid()} | {error, _Reason}.

start_link(Namespace, Client, SessionOpts, KeeperOpts, RegistryOpts) ->
    supervisor:start_link(
        {local, Namespace},
        ?MODULE,
        {Namespace, Client, SessionOpts, KeeperOpts, RegistryOpts}
    ).

%%

-spec init(Opts) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} when
        Opts :: {
            consuela_registry:namespace(),
            consuela_client:t(),
            session_opts(),
            consuela_session_keeper:opts(),
            consuela_registry:opts()
        }.

init({Namespace, Client, SessionOpts, KeeperOpts, RegistryOpts}) ->
    Session = mk_session(SessionOpts, Client),
    {ok, {
        #{strategy => one_for_all, intensity => 0, period => 1},
        [
            #{
                id    => keeper,
                start => {consuela_session_keeper, start_link, [Session, Client, KeeperOpts]}
            },
            #{
                id    => registry,
                start => {consuela_registry, start_link, [Namespace, Session, Client, RegistryOpts]}
            }
        ]
    }}.

mk_session(#{name := Name, node := Node, ttl := TTL}, Client) ->
    consuela_session:create(Name, Node, TTL, delete, Client).
