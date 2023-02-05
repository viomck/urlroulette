#!/usr/bin/env bash

for i in {1..2500}
do
    curl \
        "http://127.0.0.1:8787" \
        -i \
        -X POST \
        -d "https://example2.org/$i"
    echo "-- $i --"
done
