-module(ct_proxy_protocol).

-behaviour(ranch_protocol).

%%

-define(DEFAULT_SOCKET_OPTS, [{packet, 0}, {active, once}]).
-define(DEFAULT_TIMEOUT, 5000).

-type activity() ::
    stop
    | ignore
    | {buffer, binary()}
    | {remote, {inet:ip_address(), inet:port_number()}}.

-type proxy_fun() :: fun((binary()) -> activity()).

-export_type([activity/0]).

%% Behaviour callbacks
-export([start_link/3, init/3]).

%% Internal state
%% ----------------------------------------------------------
%% Callbacks
%% ----------------------------------------------------------

-type opts() :: #{
    proxy := proxy_fun(),
    timeout => timeout(),
    source_opts => list(gen_tcp:option()),
    remote_opts => list(gen_tcp:option())
}.

-spec start_link(pid(), module(), opts()) -> {ok, pid()}.
start_link(ListenerPid, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [ListenerPid, Transport, Opts]),
    {ok, Pid}.

%%

-record(state, {
    socket :: inet:socket(),
    socket_opts = ?DEFAULT_SOCKET_OPTS :: list(gen_tcp:option()),
    transport :: module(),
    proxy :: proxy_fun(),
    buffer = <<>> :: binary(),
    remote_endpoint :: any(),
    remote_socket :: inet:socket(),
    remote_transport :: module(),
    remote_socket_opts = ?DEFAULT_SOCKET_OPTS :: list(gen_tcp:option()),
    timeout :: non_neg_integer()
}).

-spec init(pid(), module(), opts()) -> no_return().
init(ListenerPid, Transport, Opts) ->
    {ok, Socket} = ranch:handshake(ListenerPid),
    ProxyFun = maps:get(proxy, Opts),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    SOpts = maps:get(source_opts, Opts, ?DEFAULT_SOCKET_OPTS),
    ROpts = maps:get(remote_opts, Opts, ?DEFAULT_SOCKET_OPTS),
    loop(#state{
        socket = Socket,
        transport = Transport,
        proxy = ProxyFun,
        timeout = Timeout,
        socket_opts = SOpts,
        remote_socket_opts = ROpts
    }).

%% ----------------------------------------------------------
%% Proxy internals
%% ----------------------------------------------------------

loop(
    State = #state{
        socket = Socket,
        transport = Transport,
        proxy = ProxyFun,
        buffer = Buffer,
        timeout = Timeout
    }
) ->
    case Transport:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            Buffer1 = <<Buffer/binary, Data/binary>>,
            case ProxyFun(Buffer1) of
                stop ->
                    terminate(State);
                ignore ->
                    loop(State);
                {buffer, NewData} ->
                    loop(State#state{buffer = NewData});
                {remote, Remote} ->
                    start_proxy_loop(State#state{
                        buffer = Buffer1,
                        remote_endpoint = Remote
                    })
            end;
        _ ->
            terminate(State)
    end.

start_proxy_loop(State = #state{remote_endpoint = Remote, buffer = Buffer}) ->
    case remote_connect(Remote) of
        {Transport, {ok, Socket}} ->
            Transport:send(Socket, Buffer),
            proxy_loop(State#state{
                remote_socket = Socket,
                remote_transport = Transport,
                buffer = <<>>
            });
        {_, {error, _Error}} ->
            terminate(State)
    end.

proxy_loop(
    State = #state{
        socket = SSock,
        transport = STrans,
        socket_opts = SOpts,
        remote_socket = RSock,
        remote_transport = RTrans,
        remote_socket_opts = ROpts
    }
) ->
    STrans:setopts(SSock, SOpts),
    RTrans:setopts(RSock, ROpts),

    receive
        {_, SSock, Data} ->
            RTrans:send(RSock, Data),
            proxy_loop(State);
        {_, RSock, Data} ->
            STrans:send(SSock, Data),
            proxy_loop(State);
        {tcp_closed, RSock} ->
            terminate(State);
        {tcp_closed, SSock} ->
            terminate_remote(State);
        _ ->
            terminate_all(State)
    end.

remote_connect({Ip, Port}) ->
    {ranch_tcp, gen_tcp:connect(Ip, Port, [binary, {packet, 0}, {delay_send, true}])}.

terminate(#state{socket = Socket, transport = Transport}) ->
    Transport:close(Socket).

terminate_remote(#state{remote_socket = Socket, remote_transport = Transport}) ->
    Transport:close(Socket),
    ok.

terminate_all(State) ->
    terminate_remote(State),
    terminate(State).
