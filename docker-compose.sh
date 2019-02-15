#!/bin/bash
cat <<EOF
version: '3.5'

services:
  consul1: &consul-server
    image: consul:latest
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
      /sbin/init

networks:
  consul:

EOF
