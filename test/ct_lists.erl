%%%

-module(ct_lists).

-export([enumerate/1]).

%%

-spec enumerate([A]) ->
    [{pos_integer(), A}].

enumerate(L) ->
    lists:zip(lists:seq(1, erlang:length(L)), L).
