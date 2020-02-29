package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
)

var (
	optHost      string
	optPort      int
	optDB        int
	optPassword  string
	optScanBatch int64
	optLimit     int64
	optPrefixes  string
)

const Others = "OTHERS"

var (
	data = map[string]int64{Others: 0}
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
	flag.Int64Var(&optScanBatch, "scan-batch", 1000, "batch size of SCAN command")
	flag.Int64Var(&optLimit, "limit", 0, "limit of total keys scanned")
	flag.StringVar(&optPrefixes, "prefixes", "prefixes.txt", "known prefixes file")
	flag.Parse()

	var content []byte
	if content, err = ioutil.ReadFile(optPrefixes); err != nil {
		return
	}

	lines := bytes.Split(content, []byte{'\n'})
	for _, line := range lines {
		lineStr := strings.TrimSpace(string(line))
		if len(lineStr) == 0 {
			continue
		}
		data[lineStr] = 0
	}

	r := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", optHost, optPort),
		DB:       optDB,
		Password: optPassword,
	})
	defer r.Close()

	if err = r.Ping().Err(); err != nil {
		return
	}

	var cursor uint64
	var total int64
	for {
		// scan keys
		var keys []string
		if keys, cursor, err = r.Scan(cursor, "", optScanBatch).Result(); err != nil {
			return
		}
		// analysis key
		for _, key := range keys {
			found := false
			for pfx, count := range data {
				if strings.HasPrefix(key, pfx) {
					found = true
					data[pfx] = count + 1
					break
				}
			}
			if !found {
				data[Others] = data[Others] + 1
				if data[Others] < 100 {
					log.Println("Not Matched:", key)
				}
			}
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

	var keys []string
	for key := range data {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return data[keys[i]] < data[keys[j]]
	})

	for _, key := range keys {
		log.Printf("Prefix: %s => %d", key, data[key])
	}

	log.Printf("Total: %d", total)
}
