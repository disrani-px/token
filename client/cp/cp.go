package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"os/exec"

	"github.com/google/uuid"
	"github.com/sdeoras/token/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Results struct {
	Filename      string
	FileSize      uint64
	FileReadTime  time.Duration
	FileWriteTime time.Duration
}

func main() {
	t0 := time.Now()
	host := flag.String("host", "0.0.0.0:7001", "grpc server host:port")
	outDir := flag.String("out-dir", "/tmp/out", "output dir")
	destinationDir := flag.String("destination-dir", "/tmp/data", "destination dir")
	sourceDir := flag.String("source-dir", "/tf/images", "source folder")
	useSystemCp := flag.Bool("use-system-cp", false, "use system cp command to copy")
	jobID := flag.String("job-id", "default", "job id")
	batchSize := flag.Int("batch-size", 100, "batch size")
	numBatches := flag.Int("num-batches", 25, "number of batches to run")
	flag.Parse()

	if !strings.Contains(*host, ":") {
		logrus.Fatal("--host needs a port number")
	}

	if *jobID == "default" {
		*jobID = uuid.New().String()
		logrus.Info("using job id:", *jobID)
	}

	logrus.Info("dialing grpc server: ", *host)
	ctx := context.Background()
	conn, err := grpc.Dial(*host, grpc.WithInsecure())
	if err != nil {
		logrus.Fatal(err)
	}
	client := proto.NewTokensClient(conn)
	logrus.Info("connected to grpc server: ", *host)

	if err := os.MkdirAll(*destinationDir, 0755); err != nil {
		logrus.Fatal(err)
	}

	var b bytes.Buffer
	bw := bufio.NewWriter(&b)
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

		for _, token := range tokens.Tokens {
			t := time.Now()

			if *useSystemCp {
				if _, err := exec.Command("/bin/cp",
					filepath.Join(*sourceDir, token),
					filepath.Join(*destinationDir, token)).Output(); err != nil {
					logrus.Fatal(err)
				}
			} else {
				image, err := ioutil.ReadFile(filepath.Join(*sourceDir, token))
				if err != nil {
					logrus.Fatal(err)
				}
				fileSize := uint64(len(image))
				readTime := time.Since(t)
				t = time.Now()

				if err = ioutil.WriteFile(filepath.Join(*destinationDir, token), image, 0666); err != nil {
					logrus.Fatal(err)
				}
				writeTime := time.Since(t)
				t = time.Now()

				Out := new(Results)
				Out.FileSize = fileSize
				Out.Filename = token
				Out.FileReadTime = readTime
				Out.FileWriteTime = writeTime

				jb, err := json.Marshal(Out)
				if err != nil {
					logrus.Fatal(err)
				}

				fmt.Fprintln(bw, string(jb))
			}

			if err := heartBeat.Check(); err != nil {
				heartBeat.Close()
				logrus.Fatal(err)
			}
		}

		// stop heartbeat-ing
		heartBeat.Close()
	}

	if err := bw.Flush(); err != nil {
		logrus.Fatal(err)
	}

	timeStamp := strconv.FormatInt(time.Now().UnixNano(), 16)
	dirName := filepath.Join(*outDir, *jobID)
	fileName := filepath.Join(dirName, *jobID+"_"+timeStamp+".json")

	if err := os.MkdirAll(dirName, 0755); err != nil {
		logrus.Fatal(err)
	}
	if err := ioutil.WriteFile(fileName, b.Bytes(), 0644); err != nil {
		logrus.Fatal(err)
	}
	logrus.Info("writing output: ", fileName)
	logrus.Info("all done: ", time.Since(t0))
}
