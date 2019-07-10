%%%
%%% Presence server
%%%
%%% A simple TCP server which only purpose is to service Consul TCP healthchecks.

-module(consuela_presence_server).

%%

-type ref()            :: _.
-type transport_opts() :: ranch:opts().

-type opts() :: #{
    transport_opts => transport_opts(),
    pulse          => {module(), _PulseOpts}
}.

-export([child_spec/2]).
-export([get_endpoint/1]).

-export_type([ref/0]).
-export_type([transport_opts/0]).
-export_type([opts/0]).

%%

-behaviour(ranch_protocol).
-export([start_link/4]).
-export([init/4]).

%%

-type beat() ::
    {{socket, {module(), inet:socket()}}, accepted}.

-export_type([beat/0]).

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

-spec child_spec(_Ref, opts()) ->
    supervisor:child_spec().

child_spec(Ref, Opts) ->
    ranch:child_spec(
        {?MODULE, Ref},
        ranch_tcp,
        maps:get(transport_opts, Opts, #{}),
        ?MODULE,
        mk_state(Opts)
    ).

-spec get_endpoint(_Ref) ->
    {ok, consuela_health:endpoint()} | {error, undefined}.

get_endpoint(Ref) ->
    case ranch:get_addr({?MODULE, Ref}) of
        {IP, Port} when is_tuple(IP), is_integer(Port) ->
            {ok, {IP, Port}};
        {undefined, _} ->
            {error, undefined}
    end.

%%

-type st() :: #{
    pulse := {module(), _PulseOpts}
}.

-spec mk_state(opts()) ->
    st().

mk_state(Opts) ->
    #{
        pulse => maps:get(pulse, Opts, {?MODULE, []})
    }.

-spec start_link(pid(), inet:socket(), module(), st()) ->
    {ok, pid()}.

start_link(ListenerPid, Socket, Transport, St) ->
    Pid = spawn_link(?MODULE, init, [ListenerPid, Socket, Transport, St]),
    {ok, Pid}.

-spec init(pid(), inet:socket(), module(), st()) ->
    _.

init(ListenerPid, Socket, Transport, St) ->
    {ok, _} = ranch:handshake(ListenerPid),
    _ = beat({{socket, {Transport, Socket}}, accepted}, St),
    ok = Transport:close(Socket),
    ok.

%%

-spec beat(beat(), st()) ->
    _.

beat(Beat, #{pulse := {Module, PulseOpts}}) ->
    % TODO handle errors?
    Module:handle_beat(Beat, PulseOpts).

-spec handle_beat(beat(), [trace]) ->
    ok.

handle_beat(Beat, [trace]) ->
    logger:debug("[~p] ~p", [?MODULE, Beat]);
handle_beat(_Beat, []) ->
    ok.
