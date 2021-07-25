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

	require.Equal(t, &core.InsertPointsQuery{
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
}
