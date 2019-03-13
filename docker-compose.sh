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
    ports:
      - "8401:8400"
      - "8501:8500"
      - "8601:8600"
      - "8601:8600/udp"
    command:
      agent -server -retry-join consul0 -client 0.0.0.0

  consul2:
    <<: *consul-server
    hostname: consul2
    ports:
      - "8402:8400"
      - "8502:8500"
      - "8602:8600"
      - "8602:8600/udp"

  consul0:
    <<: *consul-server
    hostname: consul0
    ports:
      - "8400:8400"
      - "8500:8500"
      - "8600:8600"
      - "8600:8600/udp"
    command:
      agent -server -bootstrap-expect 3 -ui -client 0.0.0.0

  discovery0: &discovery
    image: dr.rbkmoney.com/rbkmoney/build:ee0028263b7663828614e3a01764a836b4018193
    networks:
      - consul
    volumes:
      - .:/opt/consuela
    working_dir:
      /opt/consuela
    command: ["/bin/bash", "-c", "./test/start-discovery-node.sh consul0"]

  discovery1:
    <<: *discovery
    command: ["/bin/bash", "-c", "./test/start-discovery-node.sh consul1"]

  discovery2:
    <<: *discovery
    command: ["/bin/bash", "-c", "./test/start-discovery-node.sh consul2"]

  consuela:
    image: ${BUILD_IMAGE}
    networks:
      - consul
    volumes:
      - .:/opt/consuela
      - $HOME/.cache:/home/$UNAME/.cache
    working_dir:
      /opt/consuela
    command:
      /usr/bin/epmd

networks:
  consul:

EOF
