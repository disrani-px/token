package main

import (
	"time"

	tf "github.com/tensorflow/tensorflow/tensorflow/go"
)

var (
	graph  *tf.Graph
	labels []string
)

type ClassifyResult struct {
	Filename    string        `json:"filename"`
	Label       string        `json:"label"`
	Conf        int           `json:"conf"`
	FileSize    uint64        `json:"filesize"`
	FileIOTime  time.Duration `json:"fileiotime"`
	ComputeTime time.Duration `json:"computetime"`
	Labels      []LabelResult `json:"labels"`
}

type LabelResult struct {
	Label       string  `json:"label"`
	Probability float32 `json:"probability"`
}

type ByProbability []LabelResult

func (a ByProbability) Len() int           { return len(a) }
func (a ByProbability) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByProbability) Less(i, j int) bool { return a[i].Probability > a[j].Probability }
