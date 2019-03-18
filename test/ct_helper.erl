%%%
%%% Common test helper facilities.

-module(ct_helper).

-include_lib("stdlib/include/assert.hrl").

-export([stop_linked/2]).

-export([await/2]).
-export([await/3]).
-export([await_n/2]).
-export([await_n/3]).

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

await(Expect, Compute, Retry) ->
    await(Expect, fun (V) -> V end, Compute, Retry).

-spec await_n(non_neg_integer(), fun(() -> non_neg_integer() | _)) ->
    _Result.

await_n(N, Compute) ->
    await_n(N, Compute, genlib_retry:linear(3, 100)).

-spec await_n(non_neg_integer(), fun(() -> non_neg_integer() | _), genlib_retry:strategy()) ->
    _Result.

await_n(N, Compute, Retry) ->
    await(N, fun erlang:length/1, Compute, Retry).

await(Expect, Extract, Compute, Retry0) ->
    V = Compute(),
    case Extract(V) of
        Expect ->
            V;
        NotYet ->
            case genlib_retry:next_step(Retry0) of
                {wait, To, Retry1} ->
                    ok = timer:sleep(To),
                    await(Expect, Extract, Compute, Retry1);
                finish ->
                    ?assertEqual(Expect, NotYet)
            end
    end.
