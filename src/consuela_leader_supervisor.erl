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

-export([start_link/3]).

%%

-spec start_link(name(), module(), _Args) ->
    {ok, pid()} | ignore | {error, Reason} when
        Reason :: {shutdown, term()} | term().

start_link(Name, Module, Args) ->
    case supervisor:start_link({via, consuela, Name}, Module, Args) of
        {ok, Pid} ->
            {ok, Pid};
        ignore ->
            ignore;
        {error, {already_started, Pid}} when is_pid(Pid), node(Pid) /= node() ->
            consuela_leader_warden:start_link(Name, Pid);
        {error, Reason} ->
            {error, Reason}
    end.
