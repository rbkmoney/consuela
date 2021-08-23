%%%
%%% Relay OS signals to another process.

-module(os_signal_relay).

-type signal() ::
    sighup
    | sigquit
    | sigabrt
    | sigalrm
    | sigterm
    | sigusr1
    | sigusr2
    | sigchld
    | sigstop
    | sigtstp.

-export([replace/1]).

-behaviour(gen_event).

-export([
    init/1,
    handle_event/2,
    handle_call/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%

-spec replace([signal()]) -> ok.
replace(Signals) ->
    case whereis(erl_signal_server) of
        %% in case of minimal mode
        undefined ->
            ok;
        _ ->
            gen_event:swap_sup_handler(
                erl_signal_server,
                {erl_signal_handler, []},
                {?MODULE, {self(), Signals}}
            )
    end.

%%

-type st() :: pid().

-spec init({pid(), [signal()]} | {{pid(), [signal()]}, ok}) -> {ok, st()}.
init({{Pid, Signals}, ok}) ->
    init({Pid, Signals});
init({Pid, Signals}) when is_pid(Pid) ->
    _ = [ok = os:set_signal(S, handle) || S <- Signals],
    {ok, Pid}.

-spec handle_event(signal(), st()) -> {ok, st()}.
handle_event(Signal, St) ->
    _ = St ! {signal, Signal},
    {ok, St}.

-spec handle_call(_Request, st()) -> {ok, ok, st()}.
handle_call(_Request, St) ->
    {ok, ok, St}.

-spec handle_info(_Info, st()) -> {noreply, st()}.
handle_info(_Info, St) ->
    {ok, St}.

-spec terminate(_Reason, st()) -> _.
terminate(_Args, _St) ->
    ok.

-spec code_change(_Vsn | {down, _Vsn}, st(), _Extra) -> {ok, st()}.
code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
