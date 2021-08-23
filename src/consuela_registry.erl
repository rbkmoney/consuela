%%%
%%% A global registry facade

-module(consuela_registry).

%%

-type namespace() :: consuela_lock:namespace().
-type session() :: consuela_session:t().
-type client() :: consuela_client:t().

%% We need a notion of _registration ID_ to overrule conflicting deregistration. It looks simpler than relying
%% on _modify index_ of a lock.

% should be unique node-wide
-type rid() :: term().
-type name() :: term().

-type reg() :: {rid(), name(), pid()}.

-opaque t() :: #{
    namespace := namespace(),
    session := session(),
    client := client()
}.

-export([new/3]).

-export([register/2]).
-export([unregister/2]).
-export([lookup/2]).

-export_type([namespace/0]).
-export_type([rid/0]).
-export_type([name/0]).
-export_type([reg/0]).
-export_type([t/0]).

%%

-spec new(namespace(), session(), client()) -> t().
new(Namespace, Session, Client) ->
    #{
        namespace => Namespace,
        session => Session,
        client => Client
    }.

-type failure() ::
    {failed | unknown, _Reason}.

-type result(T) ::
    {done, T} | {failed, failure()}.

-spec register(reg(), t()) -> result(ok | {error, exists}).

-spec unregister(reg(), t()) -> result(ok | {error, stale}).

-spec lookup(name(), t()) -> result({ok, pid()} | {error, notfound}).

register({Rid, Name, Pid}, Registry = #{session := #{id := Sid}, client := Client}) ->
    ID = mk_lock_id(Name, Registry),
    try consuela_lock:hold(ID, {Rid, Pid}, Sid, Client) of
        ok ->
            % Store registration locally and monitor it
            {done, ok};
        {error, failed} ->
            % Someone on another node probably took it already
            {done, {error, exists}}
    catch
        error:{Class, Reason} when Class == failed; Class == unknown ->
            {failed, {Class, Reason}}
    end.

unregister({Rid, Name, Pid}, Registry = #{session := #{id := Sid}, client := Client}) ->
    ID = mk_lock_id(Name, Registry),
    try
        case consuela_lock:get(ID, Client) of
            {ok, Lock = #{value := {Rid, Pid}, session := Sid}} ->
                % Looks like the lock is still ours
                {done, consuela_lock:delete(Lock, Client)};
            {ok, #{value := _, session := _}} ->
                % Looks like someone (even us) is quick enough to hold it already, as the result of client
                % retrying for example
                {done, ok};
            {ok, #{}} ->
                % Looks like lock was released, nothing prohibits it to be taken again in the future
                {done, ok};
            {error, notfound} ->
                % Looks like we already denounced it
                {done, ok}
        end
    catch
        error:{Class, Reason} when Class == failed; Class == unknown ->
            {failed, {Class, Reason}}
    end.

lookup(Name, #{namespace := Namespace, client := Client}) ->
    % TODO
    % Allow to tune read consistency?
    try consuela_lock:get({Namespace, Name}, Client) of
        {ok, #{value := {_Rid, Pid}, session := _}} when is_pid(Pid) ->
            % The lock is still held by some session
            {done, {ok, Pid}};
        {ok, #{value := undefined}} ->
            % The lock was probably released
            {done, {error, notfound}};
        {error, notfound} ->
            {done, {error, notfound}}
    catch
        error:{Class, Reason} when Class == failed; Class == unknown ->
            {failed, {Class, Reason}}
    end.

mk_lock_id(Name, #{namespace := Namespace}) ->
    {Namespace, Name}.
