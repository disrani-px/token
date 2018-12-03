package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	tf "github.com/tensorflow/tensorflow/tensorflow/go"
)

func getFileList(inputDir string) ([]string, error) {
	var tokens []string

	files, err := ioutil.ReadDir(inputDir)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if !f.IsDir() && strings.ToLower(filepath.Ext(f.Name())) == ".jpg" {
			tokens = append(tokens, f.Name())
		}
	}

	return tokens, nil
}

func runWithoutScheduler() error {
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

	filenames, err := getFileList(*inDir)
	if err != nil {
		return err
	}

	// loop over number of batches
	for i := 0; i < *numBatches; i++ {

		// request tokens from server
		logrus.Info("requested tokens: ", *batchSize)

		start := i * (*batchSize)

		if start >= len(filenames) {
			break
		}

		end := (i + 1) * (*batchSize)
		if end > len(filenames) {
			end = len(filenames)
		}

		tokens := filenames[start:end]

		if len(tokens) == 0 {
			break
		}

		// loop over tokens
		logrus.Info("computing")
		t := time.Now()
		for _, token := range tokens {
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

			if nn, err := bw.Write(jb); err != nil {
				logrus.Error("error in writing to bw0:", err, ", ", fileName)
				continue
			} else {
				if nn != len(jb) {
					logrus.Error("error in writing to bw0:", err, ", ", fileName)
					continue
				}
			}

			bb := []byte("\n")
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
		logrus.Info("client looping over tokens took: ", time.Since(t), ", for jobID: ", *jobID, ", batch: ", i)
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
