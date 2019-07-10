#!/bin/bash

CONSUL=${1:-consul0}
HOSTNAME=$(hostname -i | awk '{print $1}')
NODENAME=${2:-discovery}
LIFETIME=${3:-90}
COOKIE=${4:-${NODENAME}}

SCRIPT=$(cat <<END
    discovery_node_runner:run(<<"${NODENAME}">>, "http://${CONSUL}:8500", ${LIFETIME}).
END
)

# Somewhat ugly hack to make test runs behave consistently well. Without it the node would terminate almost
# every _clean_ testrun because there's no compiled beam files from `test/` code yet.
TESTSRCDIR=_build/test/lib/consuela/test
while [ ! -f "${TESTSRCDIR}/discovery_node_runner.beam" ]; do sleep 1; done

erl \
    -noshell \
    -noinput \
    -config test/discovery \
    -setcookie "${COOKIE}" \
    -pa _build/test/lib/*/ebin ${TESTSRCDIR} \
    -name "${NODENAME}@${HOSTNAME}" \
    -eval "${SCRIPT}"
