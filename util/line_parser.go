package util

import (
	"errors"
	"regexp"
	"simpletsdb/core"
	"strconv"
	"strings"
)

var (
	lineMatchRe      = regexp.MustCompile(`^\s*([a-zA-Z0-9\-_.]+)\s*,\s*((?:[a-zA-Z0-9\-_.]+\s*=\s*[a-zA-Z0-9\-_.]+\s*)*)\s*,\s*[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)\s+([0-9]+)\s*$`)
	errNoMatches     = errors.New("parse line: invalid line protocol syntax - no matches")
	errInvalidSyntax = errors.New("parse line: invalid line protocol syntax")
)

func ParseLine(line []byte) (*core.InsertPointQuery, error) {
	strs := lineMatchRe.FindAllSubmatch(line, -1)
	if len(strs) != 1 {
		return nil, errNoMatches
	}

	match := strs[0]

	if len(match) != 6 {
		return nil, errInvalidSyntax
	}

	metric := string(match[1])

	tagsStrs := strings.Split(string(match[2]), " ")
	tags := map[string]string{}

	for _, s := range tagsStrs {
		if s == "" {
			break
		}
		s0 := strings.Split(s, "=")
		key := strings.Trim(s0[0], " \t")
		val := strings.Trim(s0[1], " \t")
		tags[key] = val
	}

	value, err := strconv.ParseFloat(string(match[3]), 64)
	if err != nil {
		return nil, err
	}

	timestamp, err := strconv.ParseInt(string(match[5]), 10, 64)
	if err != nil {
		return nil, err
	}

	return &core.InsertPointQuery{
		Metric: metric,
		Tags:   tags,
		Point: &core.Point{
			Value:     value,
			Timestamp: timestamp,
		},
	}, nil
}
