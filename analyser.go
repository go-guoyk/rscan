package main

import (
	"sort"
	"strings"
)

type AnalyserSample struct {
	Prefix string
	Count  int64
}

type Analyser struct {
	max     int64
	samples map[string]int64
}

func NewAnalyser(max int64) *Analyser {
	return &Analyser{max: max, samples: map[string]int64{}}
}

func (a *Analyser) Add(key string) {
	idx := strings.LastIndex(key, ".")
	if idx > 0 {
		key = key[0:idx]
	}
	a.samples[key] = a.samples[key] + 1
}

func (a *Analyser) Samples() (ret []AnalyserSample) {
	for pfx, count := range a.samples {
		ret = append(ret, AnalyserSample{
			Prefix: pfx,
			Count:  count,
		})
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Count < ret[j].Count
	})
	return
}
