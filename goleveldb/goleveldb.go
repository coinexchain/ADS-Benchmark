package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/coinexchain/randsrc"
	dbm "github.com/tendermint/tm-db"

	"github.com/coinexchain/ADS-benchmark/util"
)

const (
	BatchSize = 10000
	SamplePos = 99
	SampleStripe = 125
)

func main() {
	if len(os.Args) != 4 || (os.Args[1] != "rp" && os.Args[1] != "rs" && os.Args[1] != "w") {
		fmt.Printf("Usage: %s w <rand-source-file> <kv-count>\n", os.Args[0])
		fmt.Printf("Usage: %s rp <sample-file> <kv-count>\n", os.Args[0])
		fmt.Printf("Usage: %s rs <sample-file> <kv-count>\n", os.Args[0])
		return
	}
	kvCount, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}

	fmt.Printf("Before Start %d\n", time.Now().UnixNano())
	db, err := dbm.NewGoLevelDB("test", ".")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if os.Args[1] == "w" {
		randFilename := os.Args[2]
		rs := randsrc.NewRandSrcFromFile(randFilename)
		RandomWrite(db, rs, kvCount)

	}

	fmt.Printf("After Load %f\n", float64(time.Now().UnixNano())/1000000000.0)
	sampleFilename := os.Args[2]
	var totalRun int
	if os.Args[1] == "rp" {
		totalRun = util.ReadSamples(sampleFilename, kvCount, func(batch []util.KVPair) {
			checkPar(db, batch)
		})
	}
	if os.Args[1] == "rs" {
		totalRun = util.ReadSamples(sampleFilename, kvCount, func(batch []util.KVPair) {
			checkSer(db, batch)
		})
	}
	fmt.Printf("totalRun: %d\n", totalRun)
	fmt.Printf("Finished %f\n", float64(time.Now().UnixNano())/1000000000.0)
}

func RandomWrite(db dbm.DB, rs randsrc.RandSrc, count int) {
	file, err := os.OpenFile("./sample.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	numBatch := count/BatchSize
	for i := 0; i < numBatch; i++ {
		batch := db.NewBatch()
		if i % 20 == 0 {
			fmt.Printf("Now %d of %d\n", i, numBatch)
		}
		for j := 0; j < BatchSize; j++ {
			k := rs.GetBytes(32)
			v := rs.GetBytes(32)
			if (j % SampleStripe) == SamplePos {
				s := fmt.Sprintf("SAMPLE %s %s\n", base64.StdEncoding.EncodeToString(k),
					base64.StdEncoding.EncodeToString(v))
				_, err := file.Write([]byte(s))
				if err != nil {
					panic(err)
				}
			}
			batch.Set(k, v)
		}
		batch.WriteSync()
	}
}

func checkPar(db dbm.DB, batch []util.KVPair) {
	if len(batch) != util.ReadBatchSize {
		panic(fmt.Sprintf("invalid size %d %d", len(batch), util.ReadBatchSize))
	}
	var wg sync.WaitGroup
	for i := 0; i < util.ReadBatchSize/util.Stripe; i++ {
		wg.Add(1)
		go func(start, end int) {
			for _, pair := range batch[start:end] {
				v, err := db.Get(pair.Key)
				if err != nil {
					panic(err)
				}
				if !bytes.Equal(v, pair.Value) {
					fmt.Printf("Not Equal for %v: ref: %v actual: %v\n", pair.Key, pair.Value, v)
				}
			}
			wg.Done()
		}(i*util.Stripe, (i+1)*util.Stripe)
	}
	wg.Wait()
}

func checkSer(db dbm.DB, batch []util.KVPair) {
	if len(batch) != util.ReadBatchSize {
		panic("invalid size")
	}
	for _, pair := range batch {
		v, err := db.Get(pair.Key)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(v, pair.Value) {
			fmt.Printf("Not Equal for %v: ref: %v actual: %v\n", pair.Key, pair.Value, v)
		}
	}
}

