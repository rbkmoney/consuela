%%%
%%% Registry
%%%
%%% NOTES
%%%
%%% We must treat literally _any_ unexpected error we receive as an evidence of _unknowness_ in the sense
%%% that we do not know for sure what state there is on the remote side (Consul, specifically), which we must
%%% reconcile eventually. There are few error classes which do not obviously produce _unknowness_, HTTP
%%% client errors, failed connect attempts among others. But we must make _undefined by default_ as our
%%% default strategy, conservative but reliable.
%%%
%%% Upon registration we're actively and synchronously trying to acquire a lock in Consul. If we fail then the
%%% process will exit as well so we're kinda safe here. Nevertheless we must reconcile any _unknowness_ if
%%% any and rollback registration.
%%%
%%% Denouncing a registration should always succeed anyway, so any _unknowness_ we get here when trying to
%%% release a lock in Consul we should reconcile through consequent retries. It means that we should keep a
%%% queue of deregistrations.
%%%

-module(consuela_registry).

%% api

-type name() :: term().

-export([start_link/4]).

-export([register/2]).
-export([unregister/2]).
-export([lookup/1]).

-export_type([name/0]).

%% gen server

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% pulse

-type beat() ::
    {{deadline_call, deadline(), call()},
        accepted |
        rejected
    } |
    {{register | unregister, {name(), pid()}},
        started   |
        succeeded |
        {failed, _Reason}
    } |
    {unexpected,
        {{call, from()} | cast | info, _Msg}
    }.

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

-define(REGISTRATION_TIMEOUT , 1000). % TODO too short?
-define(REGISTRATION_ETC     , 100).  % TODO too short?

-type namespace() :: consuela_lock:namespace().

-type opts() :: #{
    pulse => {module(), _PulseOpts}
}.

-spec start_link(namespace(), consuela_session:t(), consuela_client:t(), opts()) ->
    {ok, pid()}.

-spec register(name(), pid()) ->
    ok | {error, exists}.

start_link(Namespace, Session, Client, Opts) ->
    % NOTE
    % Registering as a singleton on the node as it's the only way for a process to work like a process
    % registry from the point of view of OTP.
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Namespace, Session, Client, Opts}, []).

register(Name, Pid) when is_pid(Pid) ->
    deadline_call({register, {Name, Pid}}, ?REGISTRATION_ETC, ?REGISTRATION_TIMEOUT).

-spec unregister(name(), pid()) ->
    ok | {error, notfound}.

unregister(Name, Pid) when is_pid(Pid) ->
    deadline_call({unregister, {Name, Pid}}, ?REGISTRATION_ETC, ?REGISTRATION_TIMEOUT).

-spec lookup(name()) ->
    {ok, pid()} | {error, notfound}.

lookup(Name) ->
    Namespace = get_cached_value(namespace),
    Client = get_cached_value(client),
    lookup(Namespace, Name, Client).

%%

-type etc()      :: pos_integer().
-type deadline() :: integer() | infinity.

-spec deadline_call(_Call, etc(), timeout()) ->
    _Result.

deadline_call(Call, ETC, Timeout) when is_integer(ETC), ETC > 0, Timeout > ETC ->
    gen_server:call(?MODULE, {deadline_call, compute_call_deadline(ETC, Timeout), Call}, Timeout).

compute_call_deadline(ETC, Timeout) when is_integer(Timeout) ->
    get_now() + Timeout - ETC.

get_now() ->
    erlang:monotonic_time(millisecond).

%%

-type cache() :: ets:tid().
-type store() :: ets:tid().

-type st() :: #{
    store     := store(),
    monitors  := #{reference() => {name(), pid()}, name() => reference()},
    namespace := namespace(),
    session   := consuela_session:t(),
    client    := consuela_client:t(),
    cache     := cache(),
    pulse     := {module(), _PulseOpts}
}.

-type from() :: {pid(), reference()}.

-spec init({namespace(), consuela_session:t(), consuela_client:t(), opts()}) ->
    {ok, st()}.

init({Namespace, Session, Client, Opts}) ->
    Store = create_local_store(),
    Cache = create_cache(),
    St = maps:fold(
        fun
            (pulse, {Module, _} = V, St) when is_atom(Module) ->
                St#{pulse => V}
        end,
        #{
            store     => Store,
            monitors  => #{},
            namespace => Namespace,
            session   => Session,
            client    => Client,
            cache     => Cache,
            pulse     => {?MODULE, []}
        },
        Opts
    ),
    ok = cache_value(namespace, Namespace),
    ok = cache_value(client, Client),
    {ok, St}.

-type call() ::
    {register | unregister, {name(), pid()}}.

-spec handle_call({deadline, deadline(), call()}, from(), st()) ->
    {reply, _Result, st()} | {noreply, st()}.

handle_call({deadline_call, Deadline, Call} = Subject, From, St) ->
    case get_now() of
        T when Deadline > T -> % infinity > integer()
            % We still have some time
            _ = beat({Subject, accepted}, St),
            handle_regular_call(Call, From, St);
        _ ->
            % We may not complete operation in time, so just reject it
            _ = beat({Subject, rejected}, St),
            {noreply, St}
    end;
handle_call(Call, From, St) ->
    _ = beat({unexpected, {{call, From}, Call}}, St),
    {noreply, St}.

-spec handle_regular_call(call(), from(), st()) ->
    {reply, _Result, st()}.

handle_regular_call({Action, {Name, Pid}}, _From, St0) when Action == register; Action == unregister ->
    handle_activity(Action, Name, Pid, St0).

-spec handle_cast(_Cast, st()) ->
    {noreply, st()}.

handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St}.

-type down() :: {'DOWN', reference(), process, pid(), _Reason}.

-spec handle_info(down(), st()) ->
    {noreply, st()}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, St0) ->
    % TODO beat?
    {ok, St1} = handle_down(MRef, Pid, St0),
    {noreply, St1};
handle_info(Info, St) ->
    _ = beat({unexpected, {info, Info}}, St),
    {noreply, St}.

-spec terminate(_Reason, st()) ->
    ok.

terminate(_Reason, _St) ->
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) ->
    {ok, st()}.

code_change(_Vsn, St, _Extra) ->
    {ok, St}.

%%

handle_activity(Action, Name, Pid, St0) ->
    Subject = {Action, {Name, Pid}},
    _ = beat({Subject, started}, St0),
    Result = case Action of
        register ->
            register(Name, Pid, St0);
        unregister ->
            unregister(Name, Pid, St0)
    end,
    case Result of
        {ok, St1} ->
            _ = beat({Subject, succeeded}, St1),
            {ok, St1};
        {error, _} = Error ->
            _ = beat({Subject, {failed, Error}}, St0),
            Error
    end.

register(Name, Pid, St) ->
    case lookup_local_store(Name) of
        {ok, Pid} ->
            % If it's already registered locally do nothing
            {ok, St};
        {error, notfound} ->
            % If it's not there time to go global
            register_global(Name, Pid, St);
        {ok, _} ->
            % If the name already taken locally error out
            {error, exists}
    end.

register_global(Name, Pid, St0 = #{namespace := Namespace, session := Session, client := Client}) ->
    case consuela_lock:hold({Namespace, Name}, Pid, Session, Client) of
        ok ->
            % Store registration locally and monitor it
            ok = store_local(Name, Pid),
            St1 = monitor_name(Name, Pid, St0),
            {ok, St1};
        {error, failed} ->
            % Someone on another node probably taken it already
            {error, exists}
    end.

unregister(Name, Pid, St) ->
    case lookup_local_store(Name) of
        {ok, Pid} ->
            % Found it, need to go global then
            unregister_global(Name, Pid, St);
        {ok, _} ->
            % There is another process with this name
            {error, notfound};
        {error, notfound} ->
            % There was no such registration
            {error, notfound}
    end.

unregister_global(Name, _Pid, St0 = #{namespace := Namespace, session := Session, client := Client}) ->
    % TODO
    % Need to make sure that value under lock is actually `Pid`? Little afraid if that happens not to be
    case consuela_lock:release({Namespace, Name}, Session, Client) of
        ok ->
            ok = remove_local(Name),
            St1 = demonitor_name(Name, St0),
            {ok, St1}
    end.

lookup(Name, Namespace, Client) ->
    % Doing local lookup first
    case lookup_local_store(Name) of
        {ok, Pid} ->
            {ok, Pid};
        {error, notfound} ->
            lookup_global(Name, Namespace, Client)
    end.

lookup_global(Namespace, Name, Client) ->
    % TODO
    % Allow to tune consistency through `start_link`'s opts?
    case consuela_lock:get({Namespace, Name}, stale, Client) of
        {ok, #{value := Pid}} ->
            {ok, Pid};
        {error, notfound} ->
            {error, notfound}
    end.

monitor_name(Name, Pid, St = #{monitors := Monitors}) ->
    MRef = erlang:monitor(process, Pid),
    St#{monitors := Monitors#{MRef => {Name, Pid}}}.

demonitor_name(Name, St = #{monitors := Monitors}) ->
    #{Name := MRef} = Monitors,
    #{MRef := {Name, _Pid}} = Monitors,
    true = erlang:demonitor(MRef, [flush]),
    St#{monitors := maps:without([Name, MRef], Monitors)}.

handle_down(MRef, Pid, St = #{monitors := Monitors}) ->
    #{MRef := {Name, Pid}} = Monitors,
    unregister(Name, Pid, St).

%%

-define(STORE, '$consuela_registry_store').

create_local_store() ->
    ets:new(?STORE, [protected, named_table, {read_concurrency, true}]).

store_local(Name, Pid) ->
    true = ets:insert_new(?STORE, [{Name, Pid}]),
    ok.

remove_local(Name) ->
    true = ets:delete(?STORE, Name),
    ok.

lookup_local_store(Name) ->
    case ets:lookup(?STORE, Name) of
        [{Name, Pid}] ->
            {ok, Pid};
        [] ->
            {error, notfound}
    end.

%%

-define(CACHE, '$consuela_registry_cache').

create_cache() ->
    ets:new(?CACHE, [protected, named_table, {read_concurrency, true}]).

cache_value(Name, Value) ->
    true = ets:insert(?CACHE, [{Name, Value}]),
    ok.

get_cached_value(Name) ->
    ets:lookup_element(?CACHE, Name, 2).

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
