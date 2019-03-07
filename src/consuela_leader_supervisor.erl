%%%
%%% Leader supervisor
%%%
%%% NOTES
%%%
%%% A simple supervisor behaviour wrapper designed to implement _only one in a cluster_ guarantee. Some node
%%% who is quick enough to register a name will run the supervisor code while those not so lucky will start
%%% special warden process which only purpose is to look for liveness of the lucky supervisor.
%%%
%%% Unfortunately we trade this simplicity for suboptimal performance in some edge cases, such as returning
%%% `ignore` or just plain invalid specification from `Module:init/1`, or losing cluster or Consul
%%% connectivity.
%%%

-module(consuela_leader_supervisor).

%%

-type name() :: consuela_registry:name().

-export([start_link/4]).

-type opts() :: #{
    pulse => {module(), _PulseOpts}
}.

-export_type([opts/0]).

%%

-type beat() ::
    {{leader, name()}, {started, pid()}} |
    consuela_leader_warden:beat().

-export_type([beat/0]).

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

-spec start_link(name(), module(), _Args, opts()) ->
    {ok, pid()} | {error, Reason} when
        Reason :: {shutdown, term()} | term().

start_link(Name, Module, Args, Opts) ->
    case supervisor:start_link({via, consuela, Name}, Module, Args) of
        {ok, Pid} ->
            _ = beat({{leader, Name}, {started, Pid}}, Opts),
            {ok, Pid};
        {error, {already_started, Pid}} when is_pid(Pid), node(Pid) /= node() ->
            consuela_leader_warden:start_link(Name, Pid, Opts);
        {error, Reason} ->
            {error, Reason}
    end.

%%

-spec beat(beat(), opts()) ->
    _.

beat(Beat, #{pulse := {Module, Opts}}) ->
    Module:handle_beat(Beat, Opts);
beat(_Beat, #{}) ->
    ok.

-spec handle_beat(beat(), [trace]) ->
    ok.

handle_beat(Beat, [trace]) ->
    logger:debug("[~p] ~p", [?MODULE, Beat]);
handle_beat(_Beat, _) ->
    ok.
