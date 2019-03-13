#!/bin/bash

CONSUL=${1:-consul0}
HOSTNAME=$(hostname -i | awk '{print $1}')
NODENAME=${2:-discovery}
LIFETIME=${3:-90}
COOKIE=${4:-${NODENAME}}

SCRIPT=$(cat <<END

    ok   = logger:set_primary_config(level, debug),
    Apps = ct_helper:ensure_app_loaded(consuela),
    ok   = ct_consul:await_ready(),

    {ok, Address} = inet:parse_address("${HOSTNAME}"),

    Client = consuela_client:new("http://${CONSUL}:8500", #{pulse => {consuela_client, [trace]}}),
    ok     = consuela_health:register(
        #{
            name     => <<"${NODENAME}">>,
            tags     => [],
            endpoint => {Address, 31337},
            checks   => [#{
                name    => <<"${NODENAME}:erlang-node">>,
                type    => {ttl, ${LIFETIME}},
                initial => passing
            }]
        },
        Client
    ),

    {ok, Pid} = consuela_discovery_server:start_link(
        <<"${NODENAME}">>,
        [],
        Client,
        #{pulse => {consuela_discovery_server, [trace]}}
    ),

    true = erlang:register(noderunner, self()),
    ok = receive done -> ok after ${LIFETIME} * 1000 -> ok end,

    ok = consuela_health:deregister(<<"${NODENAME}">>, Client),

    init:stop().

END
)

erl \
    -noshell \
    -noinput \
    -config test/discovery \
    -setcookie "${COOKIE}" \
    -pa _build/test/lib/*/ebin _build/test/lib/consuela/test \
    -name "${NODENAME}@${HOSTNAME}" \
    -eval "${SCRIPT}"
