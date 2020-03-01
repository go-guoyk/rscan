package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
)

type KeyScanner func(key string) (err error)

var (
	ErrStopScanning = errors.New("stop scanning")
)

type Summary struct {
	Count       int64
	Type        string
	SamplesSize int64
	Samples     int64
	MultiType   bool
}

var (
	optHost        string
	optPort        int
	optDB          int
	optPassword    string
	optPrefixes    string
	optScanBatch   int64
	optMaxScans    int64
	optMaxUnknowns int64
	optMaxSamples  int64
)

var (
	rc *redis.Client

	knows        = map[string]*Summary{}
	unknowns     []string
	moreUnknowns bool

	countTotal   int64
	countKnown   int64
	countUnknown int64
)

func loadKnows() (err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(optPrefixes); err != nil {
		return
	}

	deDup := map[string]bool{}

	lines := strings.Split(string(buf), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		deDup[line] = true
	}

	for k := range deDup {
		knows[k] = &Summary{}
	}

	return
}

func createKnownPrefixes() (ret []string) {
	for key := range knows {
		ret = append(ret, key)
	}
	return
}

func sortKnownPrefixesByLength(knownPrefixes []string) {
	sort.Slice(knownPrefixes, func(i, j int) bool {
		return len(knownPrefixes[i]) > len(knownPrefixes[j])
	})
	return
}

func sortKnownPrefixesByCount(knownPrefixes []string) {
	sort.Slice(knownPrefixes, func(i, j int) bool {
		return knows[knownPrefixes[i]].Count < knows[knownPrefixes[j]].Count
	})
	return
}

func loadRedis() (err error) {
	rc = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", optHost, optPort),
		DB:       optDB,
		Password: optPassword,
	})

	if err = rc.Ping().Err(); err != nil {
		_ = rc.Close()
		rc = nil
		return
	}
	return
}

func scanKeys(scanner KeyScanner) (err error) {
	var cursor uint64
	for {
		// scan
		var keys []string
		if keys, cursor, err = rc.Scan(cursor, "", optScanBatch).Result(); err != nil {
			return
		}
		// send to scanner
		for _, key := range keys {
			if err = scanner(key); err != nil {
				break
			}
		}
		// end of scan
		if cursor == 0 {
			break
		}
	}
	return
}

func inspectKey(key string) (typ string, size int64, err error) {
	if typ, err = rc.Type(key).Result(); err != nil {
		return
	}
	switch typ {
	case "string":
		size, err = rc.StrLen(key).Result()
	case "set":
		size, err = rc.SCard(key).Result()
	case "list":
		size, err = rc.LLen(key).Result()
	case "zset":
		size, err = rc.ZCard(key).Result()
	case "hash":
		size, err = rc.HLen(key).Result()
	default:
		size = 0
	}

	return
}

func exit(err *error) {
	if *err != nil {
		log.Printf("exited with error: %s", (*err).Error())
		os.Exit(-1)
	}
}

func main() {
	var err error
	defer exit(&err)

	flag.StringVar(&optHost, "host", "127.0.0.1", "redis 主机")
	flag.IntVar(&optPort, "port", 6379, "redis 端口")
	flag.IntVar(&optDB, "db", 0, "redis 数据库")
	flag.StringVar(&optPassword, "password", "", "redis 密码")
	flag.StringVar(&optPrefixes, "prefixes", "prefixes.txt", "前缀文件，每一行为一条前缀")
	flag.Int64Var(&optScanBatch, "scan-batch", 1000, "单次 SCAN 的数量")
	flag.Int64Var(&optMaxScans, "max-scans", 0, "最大扫描多少个键, 0 代表无限制，扫描整个库, 默认为 0")
	flag.Int64Var(&optMaxUnknowns, "max-unknowns", 200, "最多记录多少条未匹配键")
	flag.Int64Var(&optMaxSamples, "max-samples", 300, "对同一个前缀，最多进行多少次采样，判定平均长度")
	flag.Parse()

	if err = loadKnows(); err != nil {
		return
	}

	if err = loadRedis(); err != nil {
		return
	}

	knownPrefixes := createKnownPrefixes()
	sortKnownPrefixesByLength(knownPrefixes)

	if err = scanKeys(func(key string) (err error) {
		// max scans
		if optMaxScans > 0 && countTotal > optMaxScans {
			err = ErrStopScanning
			return
		}

		// total counter
		countTotal++

		// known
		for _, pfx := range knownPrefixes {
			if strings.HasPrefix(key, pfx) {
				// known counter
				countKnown++
				// update summary
				summary := knows[pfx]
				// count
				summary.Count++
				// sample
				if summary.Samples < optMaxSamples {
					var typ string
					var size int64
					if typ, size, err = inspectKey(key); err != nil {
						return
					}
					// type
					if summary.Type == "" {
						summary.Type = typ
					} else if summary.Type != typ {
						summary.MultiType = true
					}
					// size
					summary.SamplesSize += size
					summary.Samples++
				}
				return
			}
		}

		// unknown
		countUnknown++
		if int64(len(unknowns)) < optMaxUnknowns {
			unknowns = append(unknowns, key)
		} else {
			moreUnknowns = true
		}
		return
	}); err != nil {
		if err == ErrStopScanning {
			log.Println("由于数量限制，扫描提前结束扫描")
			err = nil
		} else {
			return
		}
	}

	// output
	log.Println("注意: 键值大小使用 STRLEN, ZCARD, SCARD, LLEN, HLEN 等计算，不等于内存占用空间")
	log.Println("注意: 基于前缀的键值大小汇总基于采样与平均值，并不精确")
	log.Println("注意: 标注为（混合类型）的前缀表明同一个前缀的键值有不同类型")
	log.Println("------------- 未匹配键 -----------------")
	for _, key := range unknowns {
		log.Printf("键名: %s", key)
		var typ string
		var size int64
		if typ, size, err = inspectKey(key); err != nil {
			return
		}
		log.Printf("键值大小: % 12d, 类型: %s", size, typ)
		log.Println("------------------------------------")
	}
	if moreUnknowns {
		log.Println("更多未匹配键已被省略")
	}
	log.Println("------------- 已匹配键 -----------------")
	sortKnownPrefixesByCount(knownPrefixes)
	for _, pfx := range knownPrefixes {
		summary := knows[pfx]
		var extra string
		if summary.MultiType {
			extra = "(混合类型)"
		}
		eas := int64(float64(summary.SamplesSize) / float64(summary.Samples))
		ets := eas * summary.Count
		log.Printf("前缀: %s", pfx)
		log.Printf("数量: % 12d, 估计平均键值大小: % 12d, 估计总键值大小: % 12d, 类型: %s%s", summary.Count, eas, ets, summary.Type, extra)
		log.Println("------------------------------------")
	}
	log.Println("------------- 总计 -----------------")
	log.Printf("总数量：%d", countTotal)
	log.Printf("未匹配键数量：%d", countUnknown)
	log.Printf("已匹配键数量：%d", countKnown)

}
