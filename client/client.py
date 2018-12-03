from __future__ import print_function
import sys
sys.path.append('./proto')
import uuid
import grpc
import config_pb2
import config_pb2_grpc
import tensorflow as tf


def get_files_from_tokens(tokens):
	files = []
	for token in tokens:
		files.append("/Users/sdeoras/Downloads/images/" + token)
	return files


# get grpc channel to connect to
channel = grpc.insecure_channel('token-server:7001')

# get client stub
stub = config_pb2_grpc.TokensStub(channel)

# define job properties to be sent to server
# to request list of files to work on
job = config_pb2.JobID()
job.ID = str(uuid.uuid4())
job.batch_size = 128

# return value is used to send "done" confirmation
# to the server
ret = config_pb2.JobID()
ret.ID = job.ID

# keep repeating as long as server has jobs to be worked on

while True:
	tokens = stub.Get(job)
	ret.key = tokens.key
	stub.Done(ret)
	if len(tokens.tokens) == 0:
		print("all done")
		break

	else:
		fileList = get_files_from_tokens(tokens.tokens)

		filename_queue = tf.train.string_input_producer(fileList)
		reader = tf.WholeFileReader()
		key, value = reader.read(filename_queue)
		images = tf.image.decode_jpeg(value, channels=3)

		with tf.Session() as sess:
			coord = tf.train.Coordinator()
			threads = tf.train.start_queue_runners(coord=coord)

			for i in range(len(fileList)):  # length of your filename list
				image = images.eval()  # here is your image Tensor :)
				print(image.shape)

			coord.request_stop()
			coord.join(threads)