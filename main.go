package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"os"
)

var (
	optHost       string
	optPort       int
	optDB         int
	optPassword   string
	optBatch      int64
	optLimit      int64
	optMaxSamples int64
)

func exit(err *error) {
	if *err != nil {
		log.Printf("exited with error: %s", (*err).Error())
		os.Exit(-1)
	}
}

func main() {
	var err error
	defer exit(&err)

	flag.StringVar(&optHost, "host", "127.0.0.1", "redis host")
	flag.IntVar(&optPort, "port", 6379, "redis port")
	flag.IntVar(&optDB, "db", 0, "redis database")
	flag.StringVar(&optPassword, "password", "", "redis password")
	flag.Int64Var(&optMaxSamples, "max-samples", 10000, "max samples in memory")
	flag.Int64Var(&optBatch, "batch", 1000, "batch size of SCAN command")
	flag.Int64Var(&optLimit, "limit", 0, "limit of total keys scanned")
	flag.Parse()

	r := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", optHost, optPort),
		DB:       optDB,
		Password: optPassword,
	})
	defer r.Close()

	if err = r.Ping().Err(); err != nil {
		return
	}

	analyzer := NewAnalyser(optMaxSamples)

	var cursor uint64
	var total int64
	for {
		// scan keys
		var keys []string
		if keys, cursor, err = r.Scan(cursor, "", optBatch).Result(); err != nil {
			return
		}
		// add keys to analyzer
		for _, key := range keys {
			analyzer.Add(key)
		}
		// total count
		total += int64(len(keys))
		log.Printf("Scaned: %d", total)
		// limit check
		if optLimit > 0 && total >= optLimit {
			log.Println("limit reached")
			break
		}
		// end of scan
		if cursor == 0 {
			break
		}
	}

	log.Println("------------------------------")

	log.Printf("Total: %d", total)
	for _, sam := range analyzer.Samples() {
		log.Printf("Prefix: %s\t => %d", sam.Prefix, sam.Count)
	}

}
