#!/usr/bin/env bash

mkdir inputFiles
mkdir outputFiles_0
mkdir outputFiles_1
mkdir outputFiles_2

for d in {1..100}; do touch inputFiles/test_${d}.txt; done

echo copying files for index 0
export TF_CONFIG='{"cluster":{"worker":["tfjob-cp-gcs-gcp-worker-0:2222","tfjob-cp-gcs-gcp-worker-1:2222","tfjob-cp-gcs-gcp-worker-2:2222"]},"task":{"type":"worker","index":0},"environment":"cloud"}'
./split.py --input_path inputFiles --output_path outputFiles_0 --file_pattern "*"

echo copying files for index 1
export TF_CONFIG='{"cluster":{"worker":["tfjob-cp-gcs-gcp-worker-0:2222","tfjob-cp-gcs-gcp-worker-1:2222","tfjob-cp-gcs-gcp-worker-2:2222"]},"task":{"type":"worker","index":1},"environment":"cloud"}'
./split.py --input_path inputFiles --output_path outputFiles_1 --file_pattern "*"

echo copying files for index 2
export TF_CONFIG='{"cluster":{"worker":["tfjob-cp-gcs-gcp-worker-0:2222","tfjob-cp-gcs-gcp-worker-1:2222","tfjob-cp-gcs-gcp-worker-2:2222"]},"task":{"type":"worker","index":2},"environment":"cloud"}'
./split.py --input_path inputFiles --output_path outputFiles_2 --file_pattern "*"

echo "input file count:"
ls -1 inputFiles | wc -l

echo "output file count:"
for d in {0..2}; do ls -1 outputFiles_${d} | wc -l; done

rm -rf inputFiles
rm -rf outputFiles_*
