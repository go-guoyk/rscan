package main

import (
	"sort"
	"testing"
)

func TestSortKnownPrefixes(t *testing.T) {
	knownPrefixes := []string{"aaaaaa", "a"}
	sort.Slice(knownPrefixes, func(i, j int) bool {
		return len(knownPrefixes[i]) > len(knownPrefixes[j])
	})
	if knownPrefixes[1] != "a" {
		t.Fatal("bad")
	}
}
