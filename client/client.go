package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sdeoras/token/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var key string

func main() {
	t := time.Now()
	var b bytes.Buffer
	bw := bufio.NewWriter(&b)

	// flag management
	host := flag.String("host", "0.0.0.0:7001", "grpc server host:port")
	outDir := flag.String("out-dir", "/tmp", "output dir")
	jobID := flag.String("job-id", "default", "job id")
	batchSize := flag.Int("batch-size", 100, "batch size")
	numBatches := flag.Int("num-batches", 25, "number of batches to run")
	computeDelay := flag.Int("compute-delay", 100, "simulate compute delay in ms")
	flag.Parse()

	if !strings.Contains(*host, ":") {
		logrus.Fatal("--host requires a port number")
	}

	if *jobID == "default" {
		*jobID = uuid.New().String()
		logrus.Info("job id: ", *jobID)
	}

	if *batchSize <= 0 {
		logrus.Fatal("--batch-size has to be a positive integer")
	}

	if *numBatches <= 0 {
		logrus.Fatal("--num-batches has to be a positive integer")
	}

	if *computeDelay < 0 {
		logrus.Fatal("--compute-delay has to be a positive integer")
	}

	// dial GRPC server
	logrus.Info("dialing grpc: ", *host)
	ctx := context.Background()
	conn, err := grpc.Dial(*host, grpc.WithInsecure())
	if err != nil {
		logrus.Fatal(err)
	}
	defer conn.Close()
	client := proto.NewTokensClient(conn)
	logrus.Info("connected to grpc server: ", *host)

	// loop over number of batches
	for i := 0; i < *numBatches; i++ {
		// request tokens from server
		logrus.Info("requesting job tokens: ", *batchSize)
		tokens, err := client.Get(ctx, &proto.JobID{ID: *jobID, BatchSize: int32(*batchSize)})
		if err != nil {
			logrus.Fatal(err)
		}
		if len(tokens.Tokens) == 0 {
			logrus.Info("received job tokens: ", len(tokens.Tokens), ", exiting")
			break
		} else {
			logrus.Info("received job tokens: ", len(tokens.Tokens))
		}

		// start heartbeat-ing with server
		heartBeat := proto.NewHeartBeat(client, *jobID, tokens.Key)
		heartBeat.Start()

		// simulate some compute time
		t := time.Now()
		for range tokens.Tokens {
			time.Sleep(time.Millisecond * time.Duration(*computeDelay))

			if err := heartBeat.Check(); err != nil {
				heartBeat.Close()
				logrus.Fatal(err)
			}

		}
		logrus.Info("looping over tokens took: ", time.Since(t), ", for jobID: ", *jobID, ", key: ", tokens.Key)

		// stop heartbeat-ing
		heartBeat.Close()

		// send done signal to server and request acknowledgement to write
		logrus.Info("send done signal to server")
		if ack, err := client.Done(ctx, &proto.JobID{Key: tokens.Key, ID: *jobID}); err != nil {
			logrus.Fatal(err)
		} else {
			if ack.Status {
				logrus.Info("received ok to write signal from server")
				for _, token := range tokens.Tokens {
					fmt.Fprintln(bw, token)
				}
			} else {
				logrus.Info("received not-ok to write signal from server")
			}
		}
	}

	// write output
	timeStamp := strconv.FormatInt(time.Now().UnixNano(), 16)
	dirName := filepath.Join(*outDir, *jobID)
	fileName := filepath.Join(dirName, *jobID+"_"+timeStamp+".json")

	if err := os.MkdirAll(dirName, 0755); err != nil {
		logrus.Fatal(err)
	}
	if err := bw.Flush(); err != nil {
		logrus.Fatal(err)
	}
	if outBytes := b.Bytes(); len(outBytes) > 0 {
		if err := ioutil.WriteFile(fileName, outBytes, 0644); err != nil {
			logrus.Fatal(err)
		}
		logrus.Info("writing output: ", fileName)
	}

	// all done
	logrus.Info("all done: ", time.Since(t))
}
