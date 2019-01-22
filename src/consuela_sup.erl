%%%
%%% Top level supervisor

-module(consuela_sup).

%%

-export([start_link/0]).

%% Supervisor

-behaviour(supervisor).
-export([init/1]).

%%

-define(SERVER, ?MODULE).

-spec start_link() ->
    {ok, pid()} | {error, _Reason}.

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%

-spec init([]) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

init([]) ->
    {ok, {
        #{strategy => one_for_all, intensity => 0, period => 1},
        []
    }}.
