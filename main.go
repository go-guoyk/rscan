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

var (
	knownPrefixes []string

	data   = map[string]int64{}
	others int64
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

	// load known prefixes
	var content []byte
	if content, err = ioutil.ReadFile(optPrefixes); err != nil {
		return
	}

	lines := bytes.Split(content, []byte{'\n'})
	for _, line := range lines {
		lineStr := strings.TrimSpace(string(line))
		if len(lineStr) > 0 {
			knownPrefixes = append(knownPrefixes, lineStr)
		}
	}

	sort.Slice(knownPrefixes, func(i, j int) bool {
		return len(knownPrefixes[i]) > len(knownPrefixes[j])
	})

	// redis client
	r := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", optHost, optPort),
		DB:       optDB,
		Password: optPassword,
	})
	defer r.Close()

	if err = r.Ping().Err(); err != nil {
		return
	}

	// scan
	var cursor uint64
	var total int64
	for {
		// scan keys
		var keys []string
		if keys, cursor, err = r.Scan(cursor, "", optScanBatch).Result(); err != nil {
			return
		}
		// analysis key
	outerLoop:
		for _, key := range keys {
			for _, pfx := range knownPrefixes {
				if strings.HasPrefix(key, pfx) {
					data[pfx]++
					continue outerLoop
				}
			}
			// not found
			others++
			if others < 100 {
				log.Println("Not Matched:", key)
			}
		}
		// total count
		total += int64(len(keys))
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

	// output
	log.Println("------------------------------")

	sort.Slice(knownPrefixes, func(i, j int) bool {
		return data[knownPrefixes[i]] < data[knownPrefixes[j]]
	})

	var known int64
	for _, pfx := range knownPrefixes {
		known += data[pfx]
		log.Printf("% 12d: %s", data[pfx], pfx)
	}

	log.Printf("Total: %d", total)
	log.Printf("Known: %d", known)
	log.Printf("Others: %d", others)
}
