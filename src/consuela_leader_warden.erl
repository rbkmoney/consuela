%%%
%%% Leader warden

-module(consuela_leader_warden).

%%

-type name() :: consuela_registry:name().

-export([start_link/2]).

%% gen server

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%%

-spec start_link(name(), pid()) ->
    {ok, pid()}.

start_link(Name, Pid) ->
    gen_server:start_link(?MODULE, {Name, Pid}, []).

%%

-define(DEFER_TIMEOUT, 1000).

-type st() :: #{
    name := name(),
    pid  := pid(),
    mref => reference(),
    tref => reference()
}.

-type from() :: {pid(), reference()}.

-spec init({name(), pid()}) ->
    {ok, st(), hibernate}.

init({Name, Pid}) ->
    St = #{
        name => Name,
        pid  => Pid
    },
    {ok, remonitor(St), hibernate}.

-type call() :: none().

-spec handle_call(call(), from(), st()) ->
    {noreply, st(), hibernate}.

handle_call(_Call, _From, St) ->
    % TODO
    % _ = beat({unexpected, {{call, From}, Call}}, St),
    {noreply, St, hibernate}.

-type cast() :: none().

-spec handle_cast(cast(), st()) ->
    {noreply, st(), hibernate}.

handle_cast(_Cast, St) ->
    % _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St, hibernate}.

-type info() ::
    {timeout, reference(), remonitor} |
    {'DOWN', reference(), process, pid(), _Reason}.

-spec handle_info(info(), st()) ->
    {stop, _Reason, st()} | {noreply, st(), hibernate}.

handle_info({'DOWN', MRef, process, Pid, Reason}, St) ->
    handle_down(MRef, Pid, Reason, St);
handle_info({timeout, TRef, Message}, St) ->
    handle_timeout(TRef, Message, St);
handle_info(_Info, St) ->
    % _ = beat({unexpected, {info, Info}}, St),
    {noreply, St, hibernate}.

-spec handle_down(reference(), pid(), _Reason, st()) ->
    {stop, _Reason, st()} | {noreply, st()}.

handle_down(MRef, Pid, Reason, St = #{mref := MRef, pid := Pid}) ->
    handle_down(Reason, maps:remove(mref, St)).

handle_down(noconnection, St) ->
    {noreply, defer_remonitor(St), hibernate};
handle_down(Reason, St) ->
    {stop, {?MODULE, {leader_lost, Reason}}, St}.

-spec handle_timeout(reference(), remonitor, st()) ->
    {noreply, st(), hibernate}.

handle_timeout(TRef, remonitor, St = #{tref := TRef}) ->
    {noreply, remonitor(maps:remove(tref, St)), hibernate}.

-spec terminate(_Reason, st()) ->
    ok.

terminate(_Reason, _St) ->
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.

%%

remonitor(St = #{pid := Pid}) ->
    false = maps:is_key(mref, St),
    MRef = erlang:monitor(process, Pid),
    St#{mref => MRef}.

defer_remonitor(St = #{}) ->
    false = maps:is_key(tref, St),
    TRef = consuela_timer:start(?DEFER_TIMEOUT, remonitor),
    St#{tref => TRef}.
