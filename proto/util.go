package proto

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

type HeartBeat struct {
	Beat   chan error
	Done   chan bool
	Client TokensClient
	JobID  string
	Key    string
}

func NewHeartBeat(client TokensClient, jobID, key string) *HeartBeat {
	h := new(HeartBeat)
	h.Beat = make(chan error)
	h.Done = make(chan bool)
	h.Client = client
	h.JobID = jobID
	h.Key = key
	return h
}

func (h *HeartBeat) Start() {
	go func(heartBeat chan error) {
		for {
			logrus.Info("client sending HeartBeat() for job id: ", h.JobID, ", key: ", h.Key)
			if ack, err := h.Client.HeartBeat(context.Background(), &JobID{ID: h.JobID, Key: h.Key}); err != nil {
				heartBeat <- err
			} else {
				if ack.Status {
					logrus.Info("client returning for job id: ", h.JobID)
					heartBeat <- nil
				}
			}

			for i := 0; i < 10; i++ {
				select {
				case <-h.Done:
					return
				default:
					time.Sleep(time.Second) // wait for a seconds
				}
			}
		}
	}(h.Beat)
}

func (h *HeartBeat) Check() error {
	select {
	case err := <-h.Beat:
		if err != nil {
			return err
		} else {
			return nil
		}
	default:
		// never block
		return nil
	}
}

func (h *HeartBeat) Close() {
	h.Done <- true
}
