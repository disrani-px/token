#!/bin/bash
set -ex
cd server
./build.sh
cd ../client
./build.sh
cd ../apps/tensorflow-inception
./build.sh
cd ../../

cd docker
./build.sh
