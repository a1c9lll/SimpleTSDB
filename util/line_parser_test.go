package util

import (
	"simpletsdb/core"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLineParser(t *testing.T) {
	insert, err := ParseLine(`test0,id=28084 type=high,18765003.4 138456387`)

	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &core.InsertPointQuery{
		Metric: "test0",
		Tags: map[string]string{
			"id":   "28084",
			"type": "high",
		},
		Point: &core.Point{
			Value:     18765003.4,
			Timestamp: 138456387,
		},
	}, insert)

	insert, err = ParseLine(`test0,,187650 138456387`)

	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &core.InsertPointQuery{
		Metric: "test0",
		Tags:   map[string]string{},
		Point: &core.Point{
			Value:     187650,
			Timestamp: 138456387,
		},
	}, insert)

	_, err = ParseLine(`111`)

	if err == nil {
		t.Fatal("expected error")
	}

	_, err = ParseLine(`test0,, 138456387`)

	if err == nil {
		t.Fatal("expected error")
	}
}
