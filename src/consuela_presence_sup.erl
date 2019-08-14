%%%
%%% Presence supervisor

-module(consuela_presence_sup).

%%

-type opts() :: #{
    name         := consuela_health:service_name(),
    address      => inet:ip_address(),
    consul       := consul_opts(),
    server_opts  => consuela_presence_server:opts(), % #{} by default
    session_opts => consuela_presence_session:opts(), % #{} by default
    shutdown     => timeout() % if not specified uses supervisor default of 5000 ms
}.

-type consul_opts() :: #{
    url  := consuela_client:url(),
    opts => consuela_client:opts() % #{} by default
}.

-export([start_link/1]).
-export([stop/1]).

-export_type([opts/0]).

%%

-spec start_link(opts()) ->
    {ok, pid()} | {error, _}.

start_link(Opts) ->
    Name = maps:get(name, Opts),
    Address = maps:get(address, Opts, guess_host_address()),
    Client = mk_consul_client(maps:get(consul, Opts)),
    genlib_adhoc_supervisor:start_link(
        #{strategy => one_for_one, intensity => 20, period => 5},
        [
            consuela_presence_server:child_spec(Name, maps:get(server_opts, Opts, #{})),
            maps:merge(
                #{
                    id    => {Name, session},
                    start => {consuela_presence_session, start_link, [
                        Name,
                        Address,
                        Name,
                        Client,
                        maps:get(session_opts, Opts, #{})
                    ]}
                },
                maps:with([shutdown], Opts)
            )
        ]
    ).

-spec mk_consul_client(consul_opts()) ->
    consuela_client:t().

mk_consul_client(Opts) ->
    Url = maps:get(url, Opts),
    consuela_client:new(Url, maps:get(opts, Opts, #{})).

-spec stop(pid()) ->
    ok.

stop(Pid) ->
    proc_lib:stop(Pid).

%%

-spec guess_host_address() ->
    inet:ip_address().

guess_host_address() ->
    guess_host_address([distribution, hostname]).

guess_host_address([Method | Rest]) ->
    case do_guess_host_address(Method) of
        Address when is_tuple(Address) ->
            Address;
        undefined ->
            guess_host_address(Rest)
    end.

do_guess_host_address(distribution) ->
    case net_kernel:longnames() of
        true ->
            Nodename = erlang:atom_to_list(erlang:node()),
            [_, Hostname] = string:split(Nodename, "@"),
            get_host_address(Hostname);
        _ ->
            undefined
    end;
do_guess_host_address(hostname) ->
    {ok, Hostname} = inet:gethostname(),
    get_host_address(Hostname).

%%

-include_lib("kernel/include/inet.hrl").

get_host_address(Hostname) ->
    case inet:parse_address(Hostname) of
        {ok, Address} ->
            Address;
        {error, einval} ->
            case inet:gethostbyname(Hostname) of
                {ok, #hostent{h_addr_list = [Address | _]}} ->
                    Address;
                {error, _} ->
                    undefined
            end
    end.
