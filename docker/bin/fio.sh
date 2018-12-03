#!/bin/bash
# requires input argument the path to do fio ono
# second arg should be the size in G
# third arg should be either r, w or rw for read, write or read-write

if [ $3 = "w" ] || [ $3 = "rw" ]; then
	fio --blocksize=4k --filename=$1/`hostname`.fio --ioengine=libaio --readwrite=write --size=${2}G --name=test --direct=0 --gtod_reduce=1 --iodepth=32  --randrepeat=1 --disable_lat=0 > $1/`hostname`.dat
fi

if [ $3 = "r" ] || [ $3 = "rw" ]; then
	fio --blocksize=4k --filename=$1/`hostname`.fio --ioengine=libaio --readwrite=read --size=${2}G --name=test --direct=0 --gtod_reduce=1 --iodepth=32  --randrepeat=1 --disable_lat=0 --readonly >> $1/`hostname`.dat
fi
