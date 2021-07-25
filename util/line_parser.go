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

func ParseLine(line string) (*core.InsertPointsQuery, error) {
	strs := lineMatchRe.FindAllStringSubmatch(line, -1)
	if len(strs) != 1 {
		return nil, errNoMatches
	}

	match := strs[0]

	if len(match) != 6 {
		return nil, errInvalidSyntax
	}

	metric := match[1]

	tagsStrs := strings.Split(match[2], " ")
	tags := map[string]string{}
	for _, s := range tagsStrs {
		s0 := strings.Split(s, "=")
		key := strings.Trim(s0[0], " \t")
		val := strings.Trim(s0[1], " \t")
		tags[key] = val
	}

	value, err := strconv.ParseFloat(match[3], 64)
	if err != nil {
		return nil, err
	}

	timestamp, err := strconv.ParseInt(match[5], 10, 64)
	if err != nil {
		return nil, err
	}

	return &core.InsertPointsQuery{
		Metric: metric,
		Tags:   tags,
		Point: &core.Point{
			Value:     value,
			Timestamp: timestamp,
		},
	}, nil
}
