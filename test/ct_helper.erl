%%%
%%% Common test helper facilities.

-module(ct_helper).

-include_lib("stdlib/include/assert.hrl").

-export([ensure_app_loaded/1]).

-export([stop_linked/2]).

-export([await/2]).
-export([await/3]).

%%

-type appname() :: atom().

-spec ensure_app_loaded(appname()) ->
    _Deps :: [appname()].

ensure_app_loaded(AppName) ->
    case application:load(AppName) of
        R when R == ok; R == {error, {already_loaded, AppName}} ->
            {ok, Deps} = application:get_key(AppName, applications),
            lists:append(lists:map(fun genlib_app:start_application/1, Deps));
        {error, Reason} ->
            exit({application_load_failed, Reason})
    end.

%%

-spec stop_linked(pid(), _Reason) ->
    ok.

stop_linked(Pid, Reason) ->
    MRef = erlang:monitor(process, Pid),
    _ = erlang:unlink(Pid),
    _ = erlang:exit(Pid, Reason),
    receive
        {'DOWN', MRef, process, Pid, _} ->
            ok
    end.

%%

-spec await(Expect, fun(() -> Expect | _)) ->
    Expect.

await(Expect, Compute) ->
    await(Expect, Compute, genlib_retry:linear(3, 100)).

-spec await(Expect, fun(() -> Expect | _), genlib_retry:strategy()) ->
    Expect.

await(Expect, Compute, Retry0) ->
    case Compute() of
        Expect ->
            Expect;
        NotYet ->
            case genlib_retry:next_step(Retry0) of
                {wait, To, Retry1} ->
                    ok = timer:sleep(To),
                    await(Expect, Compute, Retry1);
                finish ->
                    ?assertEqual(Expect, NotYet)
            end
    end.
