package main

// code snippets were taken from: https://outcrawl.com/image-recognition-api-go-tensorflow/

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sdeoras/token/proto"
	"github.com/sirupsen/logrus"
	tf "github.com/tensorflow/tensorflow/tensorflow/go"
	"google.golang.org/grpc"
)

func runWithScheduler() error {
	t0 := time.Now()
	var b bytes.Buffer
	bw := bufio.NewWriter(&b)

	if *jobID == "default" {
		*jobID = uuid.New().String()
		logrus.Info("using job id:", *jobID)
	}

	// load tf graph and start a session
	logrus.Info("loading graph")
	if err := loadGraph(); err != nil {
		return err
	}
	logrus.Info("graph loaded")

	logrus.Info("starting session")
	dirName := *inDir
	session, err := tf.NewSession(graph, nil)
	if err != nil {
		return err
	}
	defer session.Close()
	logrus.Info("started session")

	// grpc dialing
	logrus.Info("dialing grpc server: ", *host)
	ctx := context.Background()
	conn, err := grpc.Dial(*host, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := proto.NewTokensClient(conn)
	logrus.Info("connected to grpc server: ", *host)

	// loop over number of batches
	for i := 0; i < *numBatches; i++ {

		// request tokens from server
		logrus.Info("requested tokens: ", *batchSize)
		tokens, err := client.Get(ctx, &proto.JobID{ID: *jobID, BatchSize: int32(*batchSize)})
		if err != nil {
			return err
		}
		if len(tokens.Tokens) == 0 {
			logrus.Info("received tokens: ", len(tokens.Tokens), ", exiting")
			break
		} else {
			logrus.Info("received tokens: ", len(tokens.Tokens))
		}

		// start heartbeat-ing
		heartBeat := proto.NewHeartBeat(client, *jobID, tokens.Key)
		heartBeat.Start()

		// loop over tokens
		logrus.Info("computing")
		var b0 bytes.Buffer
		bw0 := bufio.NewWriter(&b0)
		t := time.Now()
		for _, token := range tokens.Tokens {
			tLoop := time.Now()

			fileName := filepath.Join(dirName, token)
			image, err := ioutil.ReadFile(fileName)
			if err != nil {
				logrus.Error("error on file read: ", err, ", ", fileName)
				continue
			}
			fileSize := uint64(len(image))
			fileIOTime := time.Since(tLoop)
			tLoop = time.Now()

			tensor, err := makeTensorFromImage(bytes.NewBuffer(image), "jpg")
			if err != nil {
				logrus.Error("error on making tensor from image: ", err, ", ", fileName)
				continue
			}

			output, err := session.Run(
				map[tf.Output]*tf.Tensor{
					graph.Operation("input").Output(0): tensor,
				},
				[]tf.Output{
					graph.Operation("output").Output(0),
				},
				nil)
			if err != nil {
				logrus.Error("error in running session:", err, ", ", fileName)
				continue
			}
			computeTime := time.Since(tLoop)

			jb, err := json.Marshal(findBestLabels(token, output[0].Value().([][]float32)[0], fileSize, fileIOTime, computeTime))
			if err != nil {
				logrus.Error("error in json marshaling:", err, ", ", fileName)
				continue
			}

			if nn, err := bw0.Write(jb); err != nil {
				logrus.Error("error in writing to bw0:", err, ", ", fileName)
				continue
			} else {
				if nn != len(jb) {
					logrus.Error("error in writing to bw0:", err, ", ", fileName)
					continue
				}
			}

			bb := []byte("\n")
			if nn, err := bw0.Write(bb); err != nil {
				logrus.Error("could not write to bytes buffer", err)
				continue
			} else {
				if nn != len(bb) {
					logrus.Error("could not write to bytes buffer", err)
					continue
				}
			}

			// check server's response to client's heartbeat
			if err := heartBeat.Check(); err != nil {
				heartBeat.Close()
				return err
			}

		}
		logrus.Info("client looping over tokens took: ", time.Since(t), ", for jobID: ", *jobID, ", key: ", tokens.Key)

		// stop heartbeat-ing
		heartBeat.Close()

		// send done confirmation to server
		if ack, err := client.Done(ctx, &proto.JobID{Key: tokens.Key, ID: *jobID}); err != nil {
			return err
		} else {
			if ack.Status {
				if err := bw0.Flush(); err != nil {
					return err
				}

				bb := b0.Bytes()
				if nn, err := bw.Write(bb); err != nil {
					logrus.Error("could not write to bytes buffer", err)
					continue
				} else {
					if nn != len(bb) {
						logrus.Error("could not write to bytes buffer", err)
						continue
					}
				}
			}
		}
	}

	// output
	timeStamp := strconv.FormatInt(time.Now().UnixNano(), 16)
	dirName = filepath.Join(*outDir, *jobID)
	fileName := filepath.Join(dirName, *jobID+"_"+timeStamp+".json")

	if err := os.MkdirAll(dirName, 0755); err != nil {
		return err
	}

	if err := bw.Flush(); err != nil {
		return err
	}
	jb := b.Bytes()
	if len(jb) > 0 {
		if err := ioutil.WriteFile(fileName, jb, 0644); err != nil {
			return err
		}
		logrus.Info("writing output: ", fileName)
	}

	// all done
	logrus.Info("all done: ", time.Since(t0))

	return nil
}
