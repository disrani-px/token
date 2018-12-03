package main

import (
	"flag"
	"log"
	"strings"

	"github.com/sirupsen/logrus"
)

var (
	useNoHost  *bool
	host       *string
	inDir      *string
	outDir     *string
	jobID      *string
	batchSize  *int
	numBatches *int
)

func main() {
	// flag management
	useNoHost = flag.Bool("use-no-host", false, "do not communicate with host")
	host = flag.String("host", "0.0.0.0:7001", "grpc server host:port")
	inDir = flag.String("input-dir", "/tf/images", "input dir")
	outDir = flag.String("out-dir", "/tf/out", "output dir")
	jobID = flag.String("job-id", "default", "job id")
	batchSize = flag.Int("batch-size", 100, "batch size")
	numBatches = flag.Int("num-batches", 25, "number of batches to run")
	flag.Parse()

	if !strings.Contains(*host, ":") {
		logrus.Fatal("--host requires a port number")
	}

	if *useNoHost {
		if err := runWithoutScheduler(); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := runWithScheduler(); err != nil {
			log.Fatal(err)
		}
	}
}
