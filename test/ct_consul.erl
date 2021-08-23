%%%

-module(ct_consul).

-export([await_ready/0]).
-export([await_ready/2]).
-export([await_ready/3]).

%%

-spec await_ready() -> ok.

-spec await_ready(consuela_session:nodename(), consuela_client:t()) -> ok.

-spec await_ready(consuela_session:nodename(), consuela_client:t(), genlib_retry:strategy()) -> ok.

await_ready() ->
    await_ready("consul0", consuela_client:new(<<"http://consul0:8500">>, #{})).

await_ready(Node, Client) ->
    await_ready(Node, Client, genlib_retry:linear(10, 3000)).

await_ready(Node, Client, Retry) ->
    ct_helper:await(
        ok,
        fun() ->
            try
                {ok, SessionID} = consuela_session:create(<<"ready">>, Node, 30, Client),
                consuela_session:destroy(SessionID, Client)
            catch
                error:Reason ->
                    {error, Reason}
            end
        end,
        Retry
    ).
