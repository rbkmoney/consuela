#!/bin/bash

CONSUL=${1:-consul0}
HOSTNAME=$(hostname -i | awk '{print $1}')
NODENAME=${2:-discovery}
LIFETIME=${3:-90}
COOKIE=${4:-${NODENAME}}

SCRIPT=$(cat <<END
    discovery_node_runner:run(<<"${NODENAME}">>, "${HOSTNAME}", "http://${CONSUL}:8500", ${LIFETIME}).
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
