package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/sdeoras/token/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	t := time.Now()
	host := flag.String("host", "0.0.0.0:7001", "host")
	action := flag.String("action", "reset",
		"action to perform: reset, rescan, shuffle, show")
	flag.Parse()

	if !strings.Contains(*host, ":") {
		logrus.Fatal("--host needs a port number")
	}

	logrus.Info("dialing grpc:", *host)
	ctx := context.Background()
	conn, err := grpc.Dial(*host, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	client := proto.NewTokensClient(conn)
	logrus.Info("connected to grpc: ", *host)

	switch strings.ToLower(*action) {
	case "reset":
		logrus.Info("sending reset request to: ", *host)
		ack, err := client.Reset(ctx, &proto.Empty{})
		if err != nil {
			log.Fatal(err)
		}
		logrus.Info("reset request completed: ", ack.N)
	case "rescan":
		logrus.Info("sending rescan request to: ", *host)
		ack, err := client.Rescan(ctx, &proto.Empty{})
		if err != nil {
			log.Fatal(err)
		}
		logrus.Info("rescan request completed: ", ack.N)
	case "shuffle":
		logrus.Info("sending shuffle request to: ", *host)
		ack, err := client.Shuffle(ctx, &proto.Empty{})
		if err != nil {
			log.Fatal(err)
		}
		logrus.Info("shuffle request completed: ", ack.N)
	case "show":
		logrus.Info("sending show request to: ", *host)
		Data, err := client.Show(ctx, &proto.Empty{})
		if err != nil {
			log.Fatal(err)
		}
		logrus.Info("show request completed")

		for _, token := range Data.Tokens {
			fmt.Println(token)
		}
	}

	logrus.Info("all done: ", time.Since(t))
}
