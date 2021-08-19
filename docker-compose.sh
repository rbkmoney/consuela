#!/bin/bash

# Latest among allowed in our production environment as of [Tue 19 Feb 2019 19:27:53 MSK]
# (https://github.com/rbkmoney/salt-main/blob/fb1a59cd/classes/app/consul/init.yml#L6)

export CONSUL_VERSION="1.4.2"

cat <<EOF
version: '3.5'

services:
  consul1: &consul-server
    image: consul:${CONSUL_VERSION}
    networks:
      - consul
    hostname: consul1
    command:
      agent -server -retry-join consul0 -client 0.0.0.0

  consul2:
    <<: *consul-server
    hostname: consul2

  consul0:
    <<: *consul-server
    hostname: consul0
    command:
      agent -server -bootstrap-expect 3 -ui -client 0.0.0.0

  discovery0: &discovery
    image: ${BUILD_IMAGE}
    networks:
      - consul
    volumes:
      - .:$PWD
    working_dir:
      $PWD
    command: /bin/bash -c './test/start-discovery-node.sh consul0'

  discovery1:
    <<: *discovery
    command: /bin/bash -c './test/start-discovery-node.sh consul1'

  discovery2:
    <<: *discovery
    command: /bin/bash -c './test/start-discovery-node.sh consul2'

  consuela:
    image: ${BUILD_IMAGE}
    networks:
      - consul
    volumes:
      - .:$PWD
      - $HOME/.cache:/home/$UNAME/.cache
    working_dir:
      $PWD
    command:
      /usr/local/bin/epmd

networks:
  consul:

EOF
