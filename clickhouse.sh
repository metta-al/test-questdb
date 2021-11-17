#!/usr/bin/env bash

sudo docker run -d --ulimit nofile=262144:262144 \
    -p 9000:9000 -p 9009:9009 -p 8123:8123 \
    -v $(pwd)/data_clickhouse:/var/lib/clickhouse yandex/clickhouse-server
