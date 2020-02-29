package main

import (
	"log"
	"testing"
)

func TestAnalyser_Add(t *testing.T) {
	a := NewAnalyser()
	a.Add("hello-world:test.key.2223432423243")
	a.Add("hello-world:test.key.2ccc32423243")
	a.Add("hello-world:test.key.333AdF34")
	log.Println(a.Samples)
}
