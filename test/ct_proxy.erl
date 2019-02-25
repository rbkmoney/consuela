-module(ct_proxy).

-export([start_link/1]).
-export([start_link/2]).
-export([mode/2]).
-export([stop/1]).

-export([proxy/2]).

%%

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%

-include_lib("kernel/include/inet.hrl").

-type endpoint() :: {inet:hostname(), inet:port_number()}.
-type mode()     :: stop | ignore | pass.

-type proxy() :: #{
    supervisor => pid(),
    driver     => pid(),
    endpoint   => endpoint()
}.

-spec start_link(endpoint()) ->
    {ok, proxy()}.

-spec start_link(endpoint(), ranch_tcp:opts()) ->
    {ok, proxy()}.

start_link(Upstream) ->
    start_link(Upstream, [{ip, {127, 0, 0, 1}}]).

start_link(Upstream, SocketOpts) ->
    Ref = make_ref(),
    {ok, SupPid} = genlib_adhoc_supervisor:start_link(#{}, []),
    {ok, DriverPid} = supervisor:start_child(
        SupPid,
        #{
            id    => {?MODULE, driver},
            start => {gen_server, start_link, [?MODULE, resolve_endpoint(Upstream), []]}
        }
    ),
    {ok, _} = supervisor:start_child(
        SupPid,
        ranch:child_spec(
            {?MODULE, Ref},
            ranch_tcp         , #{num_acceptors => 4, socket_opts => SocketOpts},
            ct_proxy_protocol , #{proxy => fun (Buffer) -> ?MODULE:proxy(DriverPid, Buffer) end}
        )
    ),
    {IP, Port} = ranch:get_addr({?MODULE, Ref}),
    {ok, #{
        supervisor => SupPid,
        driver     => DriverPid,
        endpoint   => {inet:ntoa(IP), Port}}
    }.

resolve_endpoint({Host, Port}) ->
    {ok, #hostent{h_addr_list = [Address | _Rest]}} = inet:gethostbyname(Host),
    {Address, Port}.

-spec mode(proxy(), mode()) ->
    {ok, mode()}.

mode(#{driver := DriverPid}, Mode) ->
    gen_server:call(DriverPid, {mode, Mode}).

-spec stop(proxy()) ->
    ok.

stop(#{supervisor := SupPid}) ->
    proc_lib:stop(SupPid).

%%

-spec proxy(pid(), binary()) ->
    ct_proxy_protocol:activity().

proxy(DriverPid, Buffer) ->
    gen_server:call(DriverPid, {proxy, Buffer}).

%%

-type st() :: #{
    mode     := mode(),
    upstream := {inet:ip_address(), inet:port_number()}
}.

-spec init(endpoint()) ->
    {ok, st()}.

init(Upstream) ->
    St = #{mode => pass, upstream => Upstream},
    {ok, St}.

-spec handle_call(_Call, _From, st()) ->
    {noreply, st()}.

handle_call({mode, Mode}, _From, St = #{mode := ModeWas}) ->
    {reply, {ok, ModeWas}, St#{mode => Mode}};
handle_call({proxy, Buffer}, _From, St) ->
    {reply, get_proxy_activity(Buffer, St), St};
handle_call(_Call, _From, St) ->
    {noreply, St}.

-spec handle_cast(_Cast, st()) ->
    {noreply, st()}.

handle_cast(_Cast, St) ->
    {noreply, St}.

-spec handle_info(_Info, st()) ->
    {noreply, st()}.

handle_info(_Info, St) ->
    {noreply, St}.

-spec terminate(_Reason, st()) ->
    _.

terminate(_Reason, _St) ->
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.

%%

get_proxy_activity(_Buffer, #{mode := stop}) ->
    stop;
get_proxy_activity(_Buffer, #{mode := ignore}) ->
    ignore;
get_proxy_activity(_Buffer, #{mode := pass, upstream := Upstream}) ->
    {remote, Upstream}.
