#!/bin/bash
mkdir -p ../docker/bin
go build -o ../docker/bin/client .

cd utils
go build -o ../../docker/bin/server-util .
cd ../

cd iotest
go build -o ../../docker/bin/iotest .
cd ../

cd cp
go build -o ../../docker/bin/cp .
cd ../

cd fio
go build -o ../../docker/bin/fio .
cd ../