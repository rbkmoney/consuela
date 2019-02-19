%%%
%%% Registry
%%%
%%% NOTES
%%%
%%% In order to register a name exactly once over whole cluster we should acquire a lock keyed by that name
%%% under this node's session. But in order to denounce a registration we should _delete_ a lock not just
%%% release it despite the fact that we pay for it with extra Consul roundtrip (not with extra consensus round
%%% though fortunately). This is because we strive to keep working set of the Consul KV store _as minimal as
%%% possible_. Whether it's the objective worth striving to is unclear yet, we should probably gather some
%%% evidence and review it.
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

-export([start_link/5]).

-export([register/3]).
-export([unregister/3]).
-export([lookup/2]).

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
    {dangling,
        {enqueued, name()} |
        {dequeued, name()} |
        {{timer, reference()}, {started, timeout()} | fired | reset}
    } |
    {unexpected,
        {{call, from()} | cast | info, _Msg}
    }.

-callback handle_beat(beat(), _PulseOpts) ->
    _.

-export([handle_beat/2]).

%%

%% ETC = Estimated Time to Completion

-define(REGISTRATION_ETC       , 100).  % TODO too short?
-define(REGISTRATION_TIMEOUT   , 1000). % TODO too short?
-define(DANGLING_RETRY_TIMEOUT , 1000). % TODO put into options

-type ref()       :: atom().
-type namespace() :: consuela_lock:namespace().

-type opts() :: #{
    pulse => {module(), _PulseOpts}
}.

-export_type([ref/0]).
-export_type([namespace/0]).
-export_type([opts/0]).

-spec start_link(ref(), namespace(), consuela_session:t(), consuela_client:t(), opts()) ->
    {ok, pid()}.

start_link(Ref, Namespace, Session, Client, Opts) ->
    gen_server:start_link({local, Ref}, ?MODULE, {Ref, Namespace, Session, Client, Opts}, []).

-spec register(ref(), name(), pid()) ->
    ok | {error, exists}.

register(Ref, Name, Pid) when is_pid(Pid) ->
    handle_result(deadline_call(Ref, {register, {Name, Pid}}, ?REGISTRATION_ETC, ?REGISTRATION_TIMEOUT)).

-spec unregister(ref(), name(), pid()) ->
    ok | {error, notfound}.

unregister(Ref, Name, Pid) when is_pid(Pid) ->
    handle_result(deadline_call(Ref, {unregister, {Name, Pid}}, ?REGISTRATION_ETC, ?REGISTRATION_TIMEOUT)).

handle_result({done, Done}) ->
    Done;
handle_result({failed, Error}) ->
    erlang:error(Error).

-spec lookup(ref(), name()) ->
    {ok, pid()} | {error, notfound}.

lookup(Ref, Name) ->
    Namespace = get_cached_value(namespace, Ref),
    Client = get_cached_value(client, Ref),
    lookup(Ref, Namespace, Name, Client).

%%

-type etc()      :: pos_integer().
-type deadline() :: integer() | infinity.

-spec deadline_call(ref(), _Call, etc(), timeout()) ->
    _Result.

deadline_call(Ref, Call, ETC, Timeout) when is_integer(ETC), ETC > 0, Timeout > ETC ->
    gen_server:call(Ref, {deadline_call, compute_call_deadline(ETC, Timeout), Call}, Timeout).

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
    dangling  := {queue:queue(name()), _TRef :: reference() | undefined},
    namespace := namespace(),
    session   := consuela_session:t(),
    client    := consuela_client:t(),
    cache     := cache(),
    pulse     := {module(), _PulseOpts}
}.

-type from() :: {pid(), reference()}.

-spec init({ref(), namespace(), consuela_session:t(), consuela_client:t(), opts()}) ->
    {ok, st()}.

init({Ref, Namespace, Session, Client, Opts}) ->
    Store = create_local_store(Ref),
    Cache = create_cache(Ref),
    St = maps:fold(
        fun
            (pulse, {Module, _} = V, St) when is_atom(Module) ->
                St#{pulse => V}
        end,
        #{
            store     => Store,
            monitors  => #{},
            dangling  => {queue:new(), undefined},
            namespace => Namespace,
            session   => Session,
            client    => Client,
            cache     => Cache,
            pulse     => {?MODULE, []}
        },
        Opts
    ),
    ok = cache_value(namespace, Namespace, St),
    ok = cache_value(client, Client, St),
    {ok, St}.

-type call() ::
    {register | unregister, {name(), pid()}}.

-spec handle_call({deadline, deadline(), call()}, from(), st()) ->
    {reply, {done, _Done} | {failed, _Failed}, st()} | {noreply, st()}.

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

handle_regular_call({Action, {Name, Pid}}, _From, St0) when
    Action == register;
    Action == unregister
->
    {Result, St1} = handle_activity(Action, Name, Pid, St0),
    {reply, Result, St1}.

-spec handle_cast(_Cast, st()) ->
    {noreply, st()}.

handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St}.

-type down() :: {'DOWN', reference(), process, pid(), _Reason}.

-spec handle_info(down() | {timeout, reference(), dangling}, st()) ->
    {noreply, st()} | {noreply, st(), 0}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, St0) ->
    % TODO beat?
    {_Result, St1} = handle_down(MRef, Pid, St0),
    {noreply, St1};
handle_info({timeout, TimerRef, dangling}, St0) ->
    St1 = handle_dangling_timer(TimerRef, St0),
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
    {Result, St1} = case Action of
        register ->
            handle_register(Name, Pid, St0);
        unregister ->
            handle_unregister(Name, Pid, St0)
    end,
    _ = beat({Subject, Result}, St1),
    {Result, St1}.

handle_register(Name, Pid, St) ->
    case lookup_local_store(Name, St) of
        {ok, Pid} ->
            % If it's already registered locally do nothing
            {{done, ok}, St};
        {error, notfound} ->
            % If it's not there time to go global
            handle_register_global(Name, Pid, St);
        {ok, _} ->
            % If the name already taken locally error out
            {{done, {error, exists}}, St}
    end.

handle_register_global(Name, Pid, St0 = #{session := #{id := Sid}, client := Client}) ->
    ID = mk_lock_id(Name, St0),
    try consuela_lock:hold(ID, Pid, Sid, Client) of
        ok ->
            % Store registration locally and monitor it
            ok = store_local(Name, Pid, St0),
            St1 = monitor_name(Name, Pid, St0),
            {{done, ok}, try_start_dangling_timer(St1)};
        {error, failed} ->
            % Someone on another node probably taken it already
            {{done, {error, exists}}, try_start_dangling_timer(St0)}
    catch
        error:{failed, _} = Reason ->
            % Nothing to reconcile anyway
            {{failed, {error, Reason}}, St0};
        error:{unknown, _} = Reason ->
            % Lock may be held from the Consul's point of view, need to ensure it will be deleted eventually
            St1 = enqueue_dangling_name(Name, Pid, St0),
            {{failed, {error, Reason}}, St1}
    end.

handle_unregister(Name, Pid, St) ->
    case lookup_local_store(Name, St) of
        {ok, Pid} ->
            % Found it, need to go global then
            handle_unregister_global(Name, Pid, St);
        {ok, _} ->
            % There is another process with this name
            {{done, {error, notfound}}, St};
        {error, notfound} ->
            % There was no such registration
            {{done, {error, notfound}}, St}
    end.

handle_unregister_global(Name, Pid, St0) ->
    {Result, St1} = ensure_delete_lock(Name, Pid, St0),
    ok = remove_local(Name, St1),
    St2 = demonitor_name(Name, St1),
    {Result, St2}.

ensure_delete_lock(Name, Pid, St0 = #{session := #{id := SessionID}, client := Client}) ->
    ID = mk_lock_id(Name, St0),
    try
        {Result, St1} = case consuela_lock:get(ID, Client) of
            {ok, Lock = #{value := Pid, session := SessionID}} ->
                % Looks like the lock is still ours
                ok = consuela_lock:delete(Lock, Client),
                {{done, ok}, St0};
            {ok, #{session := AnotherID}} when SessionID /= AnotherID ->
                % Looks like someone else is quick enough to hold it already
                {{done, ok}, St0};
            {error, notfound} ->
                % Looks like we already denounced it
                {{done, ok}, St0}
        end,
        {Result, try_start_dangling_timer(St1)}
    catch
        error:{Class, Reason} when Class == failed; Class == unknown ->
            {{failed, {error, Reason}}, enqueue_dangling_name(Name, Pid, St0)}
    end.

mk_lock_id(Name, #{namespace := Namespace}) ->
    {Namespace, Name}.

lookup(Ref, Namespace, Name, Client) ->
    % Doing local lookup first
    case lookup_local_store(Name, Ref) of
        {ok, Pid} ->
            {ok, Pid};
        {error, notfound} ->
            % TODO
            % Allow to tune consistency through `start_link`'s opts?
            lookup_global(Name, Namespace, Client)
    end.

lookup_global(Name, Namespace, Client) ->
    case consuela_lock:get({Namespace, Name}, Client) of
        {ok, #{value := Pid, session := _}} when is_pid(Pid) ->
            % The lock is still held by some session
            {ok, Pid};
        {ok, #{value := undefined}} ->
            % The lock was probably released
            {error, notfound};
        {error, notfound} ->
            {error, notfound}
    end.

%%

enqueue_dangling_name(Name, Pid, St0 = #{dangling := {Queue, TimerRef}}) ->
    Dangling = {Name, Pid},
    St1 = St0#{dangling := {queue:in(Dangling, Queue), TimerRef}},
    _ = beat({dangling, {enqueued, Dangling}}, St0),
    try_start_dangling_timer(?DANGLING_RETRY_TIMEOUT, St1).

try_start_dangling_timer(St) ->
    try_start_dangling_timer(0, St).

try_start_dangling_timer(Timeout, St = #{dangling := {Queue, _}}) ->
    case queue:len(Queue) of
        N when N > 0 ->
            start_dangling_timer(Timeout, try_reset_dangling_timer(St));
        0 ->
            St
    end.

try_reset_dangling_timer(St = #{dangling := {Queue, TimerRef}}) when is_reference(TimerRef) ->
    ok = consuela_timer:reset(TimerRef),
    _ = beat({dangling, {{timer, TimerRef}, reset}}, St),
    St#{dangling := {Queue, undefined}};
try_reset_dangling_timer(St = #{dangling := {_, undefined}}) ->
    St.

start_dangling_timer(Timeout, St = #{dangling := {Queue, undefined}}) ->
    TimerRef = consuela_timer:start(Timeout, dangling),
    _ = beat({dangling, {{timer, TimerRef}, {started, Timeout}}}, St),
    St#{dangling := {Queue, TimerRef}}.

handle_dangling_timer(TimerRef, St0 = #{dangling := {Q0, TimerRef}}) ->
    _ = beat({dangling, {{timer, TimerRef}, fired}}, St0),
    case queue:out(Q0) of
        {{value, {Name, Pid} = Dangling}, Q1} ->
            St1 = St0#{dangling := {Q1, undefined}},
            _ = beat({dangling, {dequeued, Dangling}}, St1),
            {_Result, St2} = ensure_delete_lock(Name, Pid, St1),
            St2;
        {empty, Q0} ->
            St0
    end.

%%

monitor_name(Name, Pid, St = #{monitors := Monitors}) ->
    MRef = erlang:monitor(process, Pid),
    St#{monitors := Monitors#{
        MRef => {Name, Pid},
        Name => MRef
    }}.

demonitor_name(Name, St = #{monitors := Monitors}) ->
    #{Name := MRef} = Monitors,
    #{MRef := {Name, _Pid}} = Monitors,
    true = erlang:demonitor(MRef, [flush]),
    St#{monitors := maps:without([Name, MRef], Monitors)}.

handle_down(MRef, Pid, St = #{monitors := Monitors}) ->
    #{MRef := {Name, Pid}} = Monitors,
    handle_activity(unregister, Name, Pid, St).

%%

mk_store_tid(Ref) ->
    RefBin = erlang:atom_to_binary(Ref, latin1),
    erlang:binary_to_atom(<<"$consuela_registry_store/", RefBin/binary>>, latin1).

create_local_store(Ref) ->
    ets:new(mk_store_tid(Ref), [protected, named_table, {read_concurrency, true}]).

store_local(Name, Pid, #{store := Tid}) ->
    true = ets:insert_new(Tid, [{Name, Pid}]),
    ok.

remove_local(Name, #{store := Tid}) ->
    true = ets:delete(Tid, Name),
    ok.

lookup_local_store(Name, #{store := Tid}) ->
    do_lookup(Name, Tid);
lookup_local_store(Name, Ref) ->
    do_lookup(Name, mk_store_tid(Ref)).

do_lookup(Name, Tid) ->
    case ets:lookup(Tid, Name) of
        [{Name, Pid}] ->
            {ok, Pid};
        [] ->
            {error, notfound}
    end.

%%

mk_cache_tid(Ref) ->
    RefBin = erlang:atom_to_binary(Ref, latin1),
    erlang:binary_to_atom(<<"$consuela_registry_cache/", RefBin/binary>>, latin1).

create_cache(Ref) ->
    ets:new(mk_cache_tid(Ref), [protected, named_table, {read_concurrency, true}]).

cache_value(Name, Value, #{cache := Tid}) ->
    true = ets:insert(Tid, [{Name, Value}]),
    ok.

get_cached_value(Name, Ref) ->
    ets:lookup_element(mk_cache_tid(Ref), Name, 2).

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
