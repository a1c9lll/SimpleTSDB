package main

import (
	"errors"
)

var (
	errWindowRequiredForMean   = errors.New("window must be set for mean aggregator")
	errWindowRequiredForSum    = errors.New("window must be set for sum aggregator")
	errWindowRequiredForMin    = errors.New("window must be set for min aggregator")
	errWindowRequiredForMax    = errors.New("window must be set for max aggregator")
	errWindowRequiredForCount  = errors.New("window must be set for count aggregator")
	errWindowRequiredForFirst  = errors.New("window must be set for first aggregator")
	errWindowRequiredForLast   = errors.New("window must be set for last aggregator")
	errWindowRequiredForMedian = errors.New("window must be set for median aggregator")
	errWindowRequiredForMode   = errors.New("window must be set for mode aggregator")
	errWindowRequiredForStdDev = errors.New("window must be set for stddev aggregator")
)

func aggregate(aggregator *aggregatorQuery, windowApplied bool, points []*point) ([]*point, bool, error) {
	var (
		windowedAggregatorApplied bool
		err                       error
	)
	switch aggregator.Name {
	case "mean":
		if !windowApplied {
			return nil, false, errWindowRequiredForMean
		}
		points = mean(points)
		windowedAggregatorApplied = true
	case "sum":
		if !windowApplied {
			return nil, false, errWindowRequiredForSum
		}
		points = sum(points)
		windowedAggregatorApplied = true
	case "min":
		if !windowApplied {
			return nil, false, errWindowRequiredForMin
		}
		points = min(points)
		windowedAggregatorApplied = true
	case "max":
		if !windowApplied {
			return nil, false, errWindowRequiredForMax
		}
		points = max(points)
		windowedAggregatorApplied = true
	case "count":
		if !windowApplied {
			return nil, false, errWindowRequiredForCount
		}
		points, err = count(aggregator.Options, points)
		if err != nil {
			return nil, false, err
		}
		windowedAggregatorApplied = true
	case "first":
		if !windowApplied {
			return nil, false, errWindowRequiredForFirst
		}
		points = first(points)
		windowedAggregatorApplied = true
	case "last":
		if !windowApplied {
			return nil, false, errWindowRequiredForLast
		}
		points = last(points)
		windowedAggregatorApplied = true
	case "median":
		if !windowApplied {
			return nil, false, errWindowRequiredForMedian
		}
		points = median(points)
		windowedAggregatorApplied = true
	case "mode":
		if !windowApplied {
			return nil, false, errWindowRequiredForMode
		}
		points = mode(points)
		windowedAggregatorApplied = true
	case "stddev":
		if !windowApplied {
			return nil, false, errWindowRequiredForStdDev
		}
		points, err = stddev(aggregator.Options, points)
		if err != nil {
			return nil, false, err
		}
		windowedAggregatorApplied = true
	case "fill":
		points, err = fill(aggregator.Options, points)
		if err != nil {
			return nil, false, err
		}
	}

	return points, windowedAggregatorApplied, nil
}
