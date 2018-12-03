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

	"github.com/google/uuid"
	"github.com/sdeoras/token/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Results struct {
	Filename        string
	FileSize        uint64
	FileIOTimeInput time.Duration
	FileIOTimeRef   time.Duration
}

func main() {
	t0 := time.Now()
	host := flag.String("host", "0.0.0.0:7001", "grpc server host:port")
	outDir := flag.String("out-dir", "/tf/output", "output dir")
	refDir := flag.String("ref-dir", "/local/images", "reference dir")
	inputDir := flag.String("input-dir", "/tf/images", "input folder")
	jobID := flag.String("job-id", "default", "job id")
	batchSize := flag.Int("batch-size", 100, "batch size")
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

	var b bytes.Buffer
	bw := bufio.NewWriter(&b)
	for i := 0; i < 1; i++ {
		logrus.Info("requesting tokens")
		tokens, err := client.Show(ctx, &proto.Empty{})
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Info("received tokens:", len(tokens.Tokens))

		if len(tokens.Tokens) == 0 {
			break
		}

		logrus.Info("computing")
		if len(tokens.Tokens) >= *batchSize {
			tokens.Tokens = tokens.Tokens[:*batchSize]
		}
		logrus.Info("working on tokens: ", len(tokens.Tokens))
		for _, token := range tokens.Tokens {
			t := time.Now()

			image, err := ioutil.ReadFile(filepath.Join(*inputDir, token))
			if err != nil {
				logrus.Fatal(err)
			}
			fileSize := uint64(len(image))
			fileIOTimeInput := time.Since(t)
			t = time.Now()

			image, err = ioutil.ReadFile(filepath.Join(*refDir, token))
			if err != nil {
				logrus.Fatal(err)
			}
			fileIOTimeRef := time.Since(t)
			t = time.Now()

			Out := new(Results)
			Out.FileSize = fileSize
			Out.Filename = token
			Out.FileIOTimeInput = fileIOTimeInput
			Out.FileIOTimeRef = fileIOTimeRef

			jb, err := json.Marshal(Out)
			if err != nil {
				logrus.Fatal(err)
			}

			fmt.Fprintln(bw, string(jb))
		}
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
