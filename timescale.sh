#!/usr/bin/env bash

docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password \
    -v $(pwd)/data_timescale:/var/lib/postgresql/data timescale/timescaledb:latest-pg12
