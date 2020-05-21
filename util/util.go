package util

import (
	"bufio"
	"encoding/base64"
	"os"
	"strings"
)


const (
	Stripe = 64
	ReadBatchSize = 64*Stripe
)

type KVPair struct {
	Key, Value []byte
}

func ReadSamples(fname string, kvCount int, batchHandler func(batch []KVPair)) int {
	file, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var readBatch [ReadBatchSize]KVPair
	idx := 0
	totalRun := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, " ")
		if len(fields) != 3 || fields[0] != "SAMPLE" {
			panic("Invalid line: "+line)
		}
		k, err := base64.StdEncoding.DecodeString(fields[1])
		if err != nil {
			panic(err)
		}
		v, err := base64.StdEncoding.DecodeString(fields[2])
		if err != nil {
			panic(err)
		}
		readBatch[idx] = KVPair{k, v}
		idx++
		if idx == ReadBatchSize {
			idx = 0
			batchHandler(readBatch[:])
			totalRun++
			if totalRun*ReadBatchSize >= kvCount {break}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
	return totalRun
}
