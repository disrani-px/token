#!/bin/bash
mkdir -p ../../docker/bin
mkdir -p ../../docker/bin/lib
go build -o ../../docker/bin/inception .
cp -r model ../../docker/bin
sudo cp /usr/local/lib/libtensorflow.so ../../docker/bin/lib
sudo cp /usr/local/lib/libtensorflow_framework.so ../../docker/bin/lib