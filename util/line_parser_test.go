package util

import (
	"simpletsdb/core"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLineParser(t *testing.T) {
	insert, err := ParseLine([]byte(`test0,id=28084 type=high,18765003.4 138456387`))

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

	insert, err = ParseLine([]byte(`test0,,187650 138456387`))

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

	insert, err = ParseLine([]byte(`test0,,null 138456387`))

	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &core.InsertPointQuery{
		Metric: "test0",
		Tags:   map[string]string{},
		Point: &core.Point{
			Value:     0,
			Timestamp: 138456387,
			Null:      true,
		},
	}, insert)

	_, err = ParseLine([]byte(`111`))

	if err == nil {
		t.Fatal("expected error")
	}

	_, err = ParseLine([]byte(`test0,, 138456387`))

	if err == nil {
		t.Fatal("expected error")
	}
}
