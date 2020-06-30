package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/coinexchain/randsrc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/core/rawdb"

	"github.com/coinexchain/ADS-benchmark/util"
)

const (
	BatchSize = 10000
	SamplePos = 99
	SampleStripe = 125
)

func main() {
	if len(os.Args) != 5 || (os.Args[1] != "rp" && os.Args[1] != "rs" && os.Args[1] != "w") {
		fmt.Printf("Usage: %s w <rand-source-file> <kv-count> <root-file>\n", os.Args[0])
		fmt.Printf("Usage: %s rp <sample-file> <kv-count> <root-file>\n", os.Args[0])
		fmt.Printf("Usage: %s rs <sample-file> <kv-count> <root-file>\n", os.Args[0])
		return
	}
	kvCount, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}

	fmt.Printf("Before Start %d\n", time.Now().UnixNano())
	db, err := rawdb.NewLevelDBDatabase("./test.db", 0, 0, "")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	triedb := trie.NewDatabase(db)

	rootFilename := os.Args[4]
	if os.Args[1] == "w" {
		tree, err := trie.New(common.Hash{}, triedb)
		if err != nil {
			panic(err)
		}
		randFilename := os.Args[2]
		rs := randsrc.NewRandSrcFromFile(randFilename)
		RandomWrite(tree, triedb, rs, kvCount, rootFilename)
	}

	dat, err := ioutil.ReadFile(os.Args[4])
	if err != nil {
		panic(err)
	}
	root, err := base64.StdEncoding.DecodeString(string(dat))
	if err != nil {
		panic(err)
	}
	var rootHash common.Hash
	copy(rootHash[:], root)
	tree, err := trie.New(rootHash, triedb)
	if err != nil {
		panic(err)
	}

	fmt.Printf("After Load %f\n", float64(time.Now().UnixNano())/1000000000.0)
	sampleFilename := os.Args[2]
	var totalRun int
	if os.Args[1] == "rp" {
		totalRun = util.ReadSamples(sampleFilename, kvCount, func(batch []util.KVPair) {
			checkPar(tree, batch)
		})
	}
	if os.Args[1] == "rs" {
		totalRun = util.ReadSamples(sampleFilename, kvCount, func(batch []util.KVPair) {
			checkSer(tree, batch)
		})
	}
	fmt.Printf("totalRun: %d\n", totalRun)
	fmt.Printf("Finished %f\n", float64(time.Now().UnixNano())/1000000000.0)
}

func RandomWrite(tree *trie.Trie, triedb *trie.Database, rs randsrc.RandSrc, count int, rootFilename string) {
	var root common.Hash
	numBatch := count/BatchSize
	file, err := os.OpenFile("./sample.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	rootFile, err := os.OpenFile(rootFilename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer rootFile.Close()
	for i := 0; i < numBatch; i++ {
		if i % 100 == 0 {
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
			tree.Update(k, v)
			//vv := tree.Get(k)
			//if !bytes.Equal(v, vv) {
			//	fmt.Printf("Not Equal\n")
			//}
		}
		root, err = tree.Commit(nil)
		if err != nil {
			panic(err)
		}
		triedb.Commit(root, false)
	}
	rootStr := base64.StdEncoding.EncodeToString(root[:])
	fmt.Printf("ROOT %s\n", rootStr)
	rootFile.Write([]byte(rootStr))
}

func checkPar(tree *trie.Trie, batch []util.KVPair) {
	if len(batch) != util.ReadBatchSize {
		panic(fmt.Sprintf("invalid size %d %d", len(batch), util.ReadBatchSize))
	}
	var wg sync.WaitGroup
	for i := 0; i < util.ReadBatchSize/util.Stripe; i++ {
		wg.Add(1)
		go func(start, end int) {
			for _, pair := range batch[start:end] {
				v, err := tree.TryGet(pair.Key)
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

func checkSer(tree *trie.Trie, batch []util.KVPair) {
	if len(batch) != util.ReadBatchSize {
		panic("invalid size")
	}
	for _, pair := range batch {
		v, err := tree.TryGet(pair.Key)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(v, pair.Value) {
			fmt.Printf("Not Equal for %v: ref: %v actual: %v\n", pair.Key, pair.Value, v)
		}
	}
}

