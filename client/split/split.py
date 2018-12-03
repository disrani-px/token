#!/usr/bin/env python
import tensorflow as tf
import os
import json
import math
import argparse
import sys
import re

FLAGS = None


def main(_):
	print("tensorflow version:", tf.__version__)

	in_dir = FLAGS.input_path
	out_dir = FLAGS.output_path
	file_pattern = FLAGS.file_pattern
	num_workers = FLAGS.replica_count
	worker_index = FLAGS.index

	tf_config = os.getenv('TF_CONFIG', "")
	print("tfconfig is:", tf_config)

	host_name = os.getenv('HOSTNAME', "")
	print("Hostname is:", host_name)

	try:
		index = int(re.sub('.*-', '', host_name))
	except:
		print("getting index from hostname failed")
		index = worker_index

	if tf_config == "":
		print("TF_CONFIG environment var is either empty or not defined")
	else:
		tf_config = json.loads(tf_config)
		num_workers = len(tf_config['cluster']['worker'])
		index = tf_config['task']['index']

	print("numWorkers:", num_workers, "this worker index:", index)

	dataset = tf.data.Dataset.list_files(os.path.join(in_dir, file_pattern), shuffle=False)
	it = dataset.make_one_shot_iterator()
	files_found = []
	with tf.Session() as sess:
		while True:
			try:
				files_found.append(sess.run(it.get_next()))
			except tf.errors.OutOfRangeError:
				break

	print("found", len(files_found), "files matching provided pattern")

	tf.gfile.MakeDirs(out_dir)

	chunkLength = math.ceil(len(files_found)/num_workers)
	chunkStarts = [x for x in range(0, len(files_found), chunkLength)]

	if len(chunkStarts) >= index:
		chunkStart = chunkStarts[index]
		chunkEnd = len(files_found)
		if chunkStart + chunkLength < len(files_found):
			chunkEnd = chunkStart + chunkLength
		files_found = files_found[chunkStart:chunkEnd]
	else:
		print("could not split file list evenly into worker count:", num_workers)
		exit(1)

	for f in files_found:
		inFile = str(f, "utf-8")
		head, tail = os.path.split(inFile)
		outFile = os.path.join(out_dir, tail)
		print("worker", index, ":", inFile, "->", outFile)
		tf.gfile.Copy(inFile, outFile, overwrite=True)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.register("type", "bool", lambda v: v.lower() == "true")
	# Flags for defining the tf.train.ClusterSpec
	parser.add_argument(
		"--input_path",
		type=str,
		default="",
		help="input path for reading files"
	)
	parser.add_argument(
		"--output_path",
		type=str,
		default="",
		help="output path for copying files"
	)
	parser.add_argument(
		"--file_pattern",
		type=str,
		default="",
		help="pattern to search files with"
	)
	parser.add_argument(
		"--replica_count",
		type=int,
		default=1,
		help="replica count for stateful set"
	)
	parser.add_argument(
		"--index",
		type=int,
		default=0,
		help="index of this worker (starts at 0)"
	)

	FLAGS, unparsed = parser.parse_known_args()
	tf.app.run(main=main, argv=[sys.argv[0]] + unparsed)
