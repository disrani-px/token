FROM ubuntu

# environment setup
# uncomment if running python clients
RUN apt-get update
RUN apt-get install fio -y
#RUN apt-get upgrade -y
#RUN apt-get install python3 -y
#RUN apt-get install python3-pip python3-dev -y
#RUN pip3 install --upgrade pip
#RUN pip3 install tensorflow
#RUN pip install grpcio-tools

# make tensorflow folders
RUN mkdir /tensorflow
RUN mkdir /tensorflow/model
RUN mkdir -p /token/bin

# add libraries and models
ADD bin/lib/libtensorflow.so /usr/local/lib
ADD bin/lib/libtensorflow_framework.so /usr/local/lib
ADD bin/model/tensorflow_inception_graph.pb /tensorflow/model/
ADD bin/model/imagenet_comp_graph_label_strings.txt /tensorflow/model/

# update ldconfig
RUN ldconfig

# add binaries
ADD bin/server /token/bin
ADD bin/client /token/bin
ADD bin/server-util /token/bin
ADD bin/iotest /token/bin
ADD bin/cp /token/bin
ADD bin/fio /token/bin
ADD bin/fio.sh /token/bin
ADD bin/inception /tensorflow

# chmod files
RUN chmod -R 755 /token/bin
RUN chmod -R 755 /tensorflow/inception
