#!/bin/bash
protoc -I . config.proto --go_out=plugins=grpc:.
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./config.proto
