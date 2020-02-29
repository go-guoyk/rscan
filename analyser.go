package main

import (
	"regexp"
)

var (
	KeyVariationPattern = regexp.MustCompile(`[A-Fa-f0-9]+$`)
)

type Analyser struct {
	Samples map[string]int64
}

func NewAnalyser() *Analyser {
	return &Analyser{Samples: map[string]int64{}}
}

func (a *Analyser) Add(key string) {
	pfx := KeyVariationPattern.ReplaceAllString(key, "")
	a.Samples[pfx] = a.Samples[pfx] + 1
}
