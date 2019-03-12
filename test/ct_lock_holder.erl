-module(ct_lock_holder).

-export([start_link/3]).

-export([get/2]).

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

-type session() :: consuela_session:id().
-type lock()    :: consuela_lock:t().
-type client()  :: consuela_client:t().

-spec start_link(_Name, session(), client()) ->
    {ok, pid()}.

start_link(Name, Session, Client) ->
    gen_server:start_link(?MODULE, #{name => Name, session => Session, client => Client}, []).

-spec get(_Name, client()) ->
    {ok, lock()} | {error, notfound}.

get(Name, Client) ->
    consuela_lock:get({<<?MODULE_STRING>>, Name}, Client).

%%

-type st() :: #{
    name    := _,
    session := session(),
    client  := client(),
    lock    => lock()
}.

-spec init(st()) ->
    {ok, st()}.

init(St = #{name := Name, session := Session, client := Client}) ->
    _Was = erlang:process_flag(trap_exit, true),
    ok = consuela_lock:hold({<<?MODULE_STRING>>, Name}, [], Session, Client),
    {ok, Lock} = consuela_lock:get({<<?MODULE_STRING>>, Name}, Client),
    {ok, St#{lock => Lock}}.

-spec handle_call(_Call, _From, st()) ->
    {noreply, st()}.

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

terminate(_Reason, _St = #{lock := Lock, client := Client}) ->
    ok = consuela_lock:delete(Lock, Client),
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.
