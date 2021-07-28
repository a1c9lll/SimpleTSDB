package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	metricAndTagsRe                = regexp.MustCompile(`^[a-zA-Z0-9_\-.]+$`)
	insertBatchSize                = 200
	errUnsupportedMetricName       = errors.New("valid characters for metrics are a-z, A-Z, 0-9, -, ., and _")
	errUnsupportedTagName          = errors.New("valid characters for tag names are a-z, A-Z, 0-9, -, ., and _")
	errUnsupportedTagValue         = errors.New("valid characters for tag values are a-z, A-Z, 0-9, -, ., and _")
	errMetricRequired              = errors.New("metric is required")
	errMetricDoesNotExist          = errors.New("metric does not exist")
	errStartRequired               = errors.New("query start is required")
	errEndRequired                 = errors.New("query end is required")
	errPgTableNotExist             = `pq: relation "simpletsdb_%s" does not exist`
	errPointRequiredForInsertQuery = errors.New("point required for insert query")
)

func generatePointInsertionStringsAndValues(queries []*insertPointQuery) (string, []interface{}, error) {
	valuesStrBuilder := &strings.Builder{}
	values := []interface{}{}

	var i = 1
	for z, query := range queries {
		if !metricAndTagsRe.MatchString(query.Metric) {
			return "", nil, errUnsupportedMetricName
		}
		if query.Point == nil {
			return "", nil, errPointRequiredForInsertQuery
		}
		values = append(values, query.Metric)
		values = append(values, query.Point.Timestamp)
		values = append(values, query.Point.Value)

		bs, _ := json.Marshal(query.Tags)
		values = append(values, string(bs))

		valuesStrBuilder.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d)", i, i+1, i+2, i+3))
		if z+1 < len(queries) {
			valuesStrBuilder.WriteString(",")
		}
		i += 4
	}
	return valuesStrBuilder.String(), values, nil
}

func insertPoints(queries0 []*insertPointQuery) error {
	if len(queries0) == 0 {
		return nil
	}
	// batch the queries 200 at a time to get around
	// max insert limit of postgres
	for i := 0; i < len(queries0); i += insertBatchSize {
		queries := queries0[i:min1(i+insertBatchSize, len(queries0))]
		valuesStr, values, err := generatePointInsertionStringsAndValues(queries)
		if err != nil {
			return err
		}
		queryStr := fmt.Sprintf(`INSERT INTO simpletsdb_metrics (metric,timestamp,value,tags) VALUES %s ON CONFLICT DO NOTHING`, valuesStr)
		if _, err := session.Exec(queryStr, values...); err != nil { //&& err.Error() != fmt.Sprintf(errStringDuplicate, strings.ToLower(firstMetric)) {
			return err
		}
	}
	return nil
}

func generateTagsQueryString(tags map[string]string, queryVals []interface{}) (string, []interface{}, error) {
	s := &strings.Builder{}
	argsCounter := 3
	for k, v := range tags {
		if !metricAndTagsRe.MatchString(k) {
			return "", nil, errUnsupportedTagName
		}
		if !metricAndTagsRe.MatchString(v) {
			return "", nil, errUnsupportedTagValue
		}
		s.WriteString(fmt.Sprintf(" AND tags->>'%s' = $%s", k, strconv.Itoa(argsCounter+1)))
		argsCounter++
		queryVals = append(queryVals, v)
	}
	return s.String(), queryVals, nil
}

func queryPoints(query *pointsQuery) ([]*point, error) {
	if query.Metric == "" {
		return nil, errMetricRequired
	}
	if !metricAndTagsRe.MatchString(query.Metric) {
		return nil, errUnsupportedMetricName
	}

	if query.Start == 0 {
		return nil, errStartRequired
	}
	if query.End == 0 {
		query.End = time.Now().UnixNano()
	}

	var (
		limitStr string
		tagStr   string
		err      error
	)

	if query.N > 0 {
		limitStr = fmt.Sprintf(" LIMIT %d", query.N)
	}

	queryVals := []interface{}{
		query.Metric, query.Start, query.End,
	}

	if len(query.Tags) > 0 {
		tagStr, queryVals, err = generateTagsQueryString(query.Tags, queryVals)
		if err != nil {
			return nil, err
		}
	}

	queryStr := fmt.Sprintf(`SELECT timestamp, value FROM simpletsdb_metrics WHERE metric = $1 AND timestamp >= $2 AND timestamp <= $3%s ORDER BY timestamp ASC%s`, tagStr, limitStr)

	scanner, err := session.Query(queryStr, queryVals...)
	var (
		value     float64
		timestamp int64
		points    []*point
	)
	if err != nil {
		if err.Error() == fmt.Sprintf(errPgTableNotExist, query.Metric) {
			return nil, errMetricDoesNotExist
		}
		return nil, err
	}
	for scanner.Next() {
		err := scanner.Scan(&timestamp, &value)
		if err != nil {
			scanner.Close()
			return nil, err
		}
		points = append(points, &point{
			Value:     value,
			Timestamp: timestamp,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	var (
		windowApplied              bool
		windowedAggregatorApplied  bool
		windowedAggregatorApplied0 bool
	)

	if query.Window != nil {
		points, err = window(query.Start, query.End, query.Window, points)
		if err != nil {
			return nil, err
		}
		windowApplied = true
	}

	for _, aggregator := range query.Aggregators {
		points, windowedAggregatorApplied0, err = aggregate(aggregator, windowApplied, points)
		if err != nil {
			return nil, err
		}
		if windowedAggregatorApplied0 {
			windowedAggregatorApplied = true
		}
	}

	// Set windows to 0 if the points are window aggregated since all of the
	// timestamps will be windows anyway
	if windowedAggregatorApplied {
		for _, pt := range points {
			pt.Window = 0
		}
	}

	return points, nil
}

func deletePoints(query *deletePointsQuery) error {
	if query.Metric == "" {
		return errMetricRequired
	}
	if !metricAndTagsRe.MatchString(query.Metric) {
		return errUnsupportedMetricName
	}
	if query.Start == 0 {
		return errStartRequired
	}
	if query.End == 0 {
		return errEndRequired
	}

	var (
		tagStr string
		err    error
	)

	queryVals := []interface{}{
		query.Metric, query.Start, query.End,
	}

	if len(query.Tags) > 0 {
		tagStr, queryVals, err = generateTagsQueryString(query.Tags, queryVals)
		if err != nil {
			return err
		}
	}

	queryStr := fmt.Sprintf(`DELETE FROM simpletsdb_metrics WHERE metric = $1 AND timestamp >= $2 AND timestamp <= $3%s`, tagStr)

	if _, err := session.Exec(queryStr, queryVals...); err != nil {
		return err
	}
	return nil
}
