package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLineParser(t *testing.T) {
	insert, err := parseLineProtocol([]byte(`test0,id=28084 type=high,18765003.4 138456387`))

	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &insertPointQuery{
		Metric: "test0",
		Tags: map[string]string{
			"id":   "28084",
			"type": "high",
		},
		Point: &point{
			Value:     18765003.4,
			Timestamp: 138456387,
		},
	}, insert)

	insert, err = parseLineProtocol([]byte(`test0,,187650 138456387`))

	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &insertPointQuery{
		Metric: "test0",
		Tags:   map[string]string{},
		Point: &point{
			Value:     187650,
			Timestamp: 138456387,
		},
	}, insert)

	_, err = parseLineProtocol([]byte(`111`))

	if err == nil {
		t.Fatal("expected error")
	}

	_, err = parseLineProtocol([]byte(`test0,, 138456387`))

	if err == nil {
		t.Fatal("expected error")
	}

	insert, err = parseLineProtocol([]byte(`test0,,-3749827 138456387`))

	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &insertPointQuery{
		Metric: "test0",
		Tags:   map[string]string{},
		Point: &point{
			Value:     -3749827,
			Timestamp: 138456387,
		},
	}, insert)
}
