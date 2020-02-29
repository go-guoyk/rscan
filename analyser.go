package main

import (
	"regexp"
	"sort"
)

var (
	KeyVariationPattern = regexp.MustCompile(`[A-Fa-f0-9]+$`)
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
	pfx := KeyVariationPattern.ReplaceAllString(key, "")
	a.samples[pfx] = a.samples[pfx] + 1
}

func (a *Analyser) Samples() (ret []AnalyserSample) {
	for pfx, count := range a.samples {
		ret = append(ret, AnalyserSample{
			Prefix: pfx,
			Count:  count,
		})
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Count > ret[j].Count
	})
	return
}
