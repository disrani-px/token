package main

import (
	"context"
	"errors"
	"flag"
	"io/ioutil"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sdeoras/token/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	Tokens      []string
	JobData     map[string]*Data
	Lock        sync.Mutex
	Folder      string
	letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

type Data struct {
	currentIndex  int
	StartTime     time.Time
	EndTime       time.Time
	Completed     bool
	TotalDuration time.Duration
	lastHeartbeat map[string]time.Time
	keyMap        map[string][]int
}

type server struct{}

func main() {
	rand.Seed(time.Now().UnixNano())

	folder := flag.String("dir", "/tf/images", "folder to scan for files")
	host := flag.String("host", ":7001", "gRPC host in host:port format")
	flag.Parse()

	if !strings.Contains(*host, ":") {
		logrus.Fatal("--host requires a port number")
	}

	// these are bookkeeping memory structures
	JobData = make(map[string]*Data)
	Tokens = make([]string, 0, 0)

	Folder = *folder
	if err := scanFiles(Folder); err != nil {
		logrus.Fatal(err)
	}

	//start grpc server on localhost
	lis, err := net.Listen("tcp", *host)
	if err != nil {
		logrus.Fatal(err)
	}
	s := grpc.NewServer()
	proto.RegisterTokensServer(s, &server{})
	reflection.Register(s)

	cerr := make(chan error)
	go func(c chan error) {
		logrus.Info("starting server")
		c <- s.Serve(lis)
	}(cerr)

	go func() {
		logrus.Info("starting cleaner bot")
		for {
			select {
			case <-time.After(time.Hour):
				logrus.WithField("count", len(Tokens)).Info("cleanup bot")
				Lock.Lock()
				tmp := make(map[string]*Data)
				for key, val := range JobData {
					if time.Since(val.StartTime) < time.Hour*24 {
						tmp[key] = val
					}
				}
				JobData = nil
				JobData = tmp
				Lock.Unlock()
			}
		}
	}()

	time.Sleep(time.Second * 2)

	// block forever on error
	logrus.Info("listening on ", *host)
	logrus.Info("ctrl-c to exit")
	logrus.Fatal(<-cerr)
}

func initJobData(id string) *Data {
	data, present := JobData[id]
	if !present {
		JobData[id] = new(Data)
		data = JobData[id]
		data.keyMap = make(map[string][]int)
		data.lastHeartbeat = make(map[string]time.Time)
		data.StartTime = time.Now()
	}
	return data
}

func (s *server) Get(ctx context.Context, job *proto.JobID) (*proto.Data, error) {
	Lock.Lock()
	defer Lock.Unlock()
	logrus.WithField("jobID", job.ID).
		WithField("signal", "get").
		Info("get request")

	data := initJobData(job.ID)
	keyMap := data.keyMap
	ind := data.currentIndex

	newkey := randStringRunes(8) // generate 8 char wide random string

	batchSize := int(job.BatchSize)
	if batchSize > len(Tokens)-ind {
		batchSize = len(Tokens) - ind
	}

	var tokens []string
	if batchSize > 0 {
		data.currentIndex += batchSize
		tokens = make([]string, batchSize)
		for i := range tokens {
			tokens[i] = Tokens[ind+i]
		}
		keyMap[newkey] = []int{ind, batchSize}
		data.Completed = false
		logrus.WithField("key", newkey).
			WithField("count", len(tokens)).
			WithField("jobID", job.ID).
			Info("assigned")
	} else {
		// try to assign previously assigned work
		if len(keyMap) > 0 {
			for key, value := range keyMap {
				// check sanity of values
				if len(value) != 2 {
					return nil, errors.New("bookkeeping fault for JobId: " + job.ID)
				}
				if value[0]+value[1] > len(Tokens) {
					return nil, errors.New("bookkeeping fault for JobId: " + job.ID)
				}
				if value[0] < 0 || value[1] < 0 {
					return nil, errors.New("bookkeeping fault for JobId: " + job.ID)
				}

				if time.Since(data.lastHeartbeat[key]) > time.Minute {
					tokens = make([]string, value[1])
					for i := range tokens {
						tokens[i] = Tokens[value[0]+i]
					}
					newkey = key
					logrus.WithField("key", newkey).
						WithField("count", len(tokens)).
						WithField("jobID", job.ID).
						Info("re-assigned")
					break
				}
			}
		} else {
			newkey = ""
			tokens = nil
			if len(keyMap) == 0 {
				logrus.WithField("jobID", job.ID).
					Info("nothing pending")
			}
		}
	}

	return &proto.Data{Tokens: tokens, Key: newkey}, nil
}

func (s *server) Reset(ctx context.Context, empty *proto.Empty) (*proto.Ack, error) {
	Lock.Lock()
	defer Lock.Unlock()
	logrus.WithField("signal", "reset").
		Info("deleting history")

	JobData = make(map[string]*Data)
	return &proto.Ack{N: int32(len(Tokens))}, nil
}

func (s *server) Rescan(ctx context.Context, empty *proto.Empty) (*proto.Ack, error) {
	if err := scanFiles(Folder); err != nil {
		return nil, err
	}
	logrus.WithField("signal", "rescan").
		WithField("count", len(Tokens)).
		Info("scanning folder")

	return &proto.Ack{N: int32(len(Tokens))}, nil
}

func (s *server) Shuffle(ctx context.Context, empty *proto.Empty) (*proto.Ack, error) {
	Lock.Lock()
	defer Lock.Unlock()

	logrus.WithField("signal", "shuffle").
		WithField("count", len(Tokens)).
		Info("shuffling tokens")
	n := len(Tokens)
	for i := 0; i < len(Tokens); i++ {
		j := rand.Intn(n)
		Tokens[i], Tokens[j] = Tokens[j], Tokens[i]
	}
	return &proto.Ack{N: int32(len(Tokens))}, nil
}

func (s *server) Show(ctx context.Context, empty *proto.Empty) (*proto.Data, error) {
	Lock.Lock()
	defer Lock.Unlock()

	logrus.WithField("signal", "show").
		WithField("count", len(Tokens)).
		Info("listing tokens")
	Out := new(proto.Data)
	Out.Tokens = make([]string, len(Tokens))
	for i, token := range Tokens {
		Out.Tokens[i] = token
	}

	return Out, nil
}

func (s *server) Done(ctx context.Context, key *proto.JobID) (*proto.Ack, error) {
	Lock.Lock()
	defer Lock.Unlock()

	logrus.WithField("signal", "done").
		WithField("jobID", key.ID).WithField("key", key.Key).
		Info("done")

	if data, present := JobData[key.ID]; !present {
		logrus.WithField("jobID", key.ID).
			Info("server could not find job id")
		return &proto.Ack{}, nil
	} else {
		if _, present = data.keyMap[key.Key]; !present {
			logrus.WithField("jobID", key.ID).
				WithField("key", key.Key).
				Info("key not found")

			return &proto.Ack{}, nil
		} else {
			logrus.WithField("jobID", key.ID).
				WithField("key", key.Key).
				Info("deleting key")
			delete(data.keyMap, key.Key)
			if len(data.keyMap) == 0 {
				data.Completed = true
				data.EndTime = time.Now()
				data.TotalDuration = time.Since(data.StartTime)
				logrus.WithField("jobID", key.ID).
					WithField("completed", data.Completed).
					WithField("duration", data.TotalDuration).Info("done")
			}
			return &proto.Ack{Status: true}, nil
		}
	}
}

func (s *server) HeartBeat(ctx context.Context, job *proto.JobID) (*proto.Ack, error) {
	Lock.Lock()
	defer Lock.Unlock()
	logrus.WithField("jobID", job.ID).
		WithField("key", job.Key).
		WithField("signal", "heartbeat").
		Info("received heartbeat")
	data, present := JobData[job.ID]
	if !present {
		return &proto.Ack{}, errors.New("job id not present")
	}
	data.lastHeartbeat[job.Key] = time.Now()
	return &proto.Ack{Status: data.Completed}, nil
}

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func scanFiles(folder string) error {
	Lock.Lock()
	defer Lock.Unlock()
	Tokens = make([]string, 0, 0)
	JobData = make(map[string]*Data)

	files, err := ioutil.ReadDir(folder)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		Tokens = append(Tokens, file.Name())
	}

	return nil
}
