package main

import (
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
	errMetricExists                = errors.New("metric already exists")
	errMetricDoesNotExist          = errors.New("metric does not exist")
	errStartRequired               = errors.New("query start is required")
	errEndRequired                 = errors.New("query end is required")
	errStringDuplicate             = `pq: duplicate key value violates unique constraint "simpletsdb_%s_timestamp_value_key"`
	errPgTableNotExist             = `pq: relation "simpletsdb_%s" does not exist`
	errPointRequiredForInsertQuery = errors.New("point required for insert query")
	errSameMetricRequiredForInsert = errors.New("all metric names must be the same in an insert")
)

func generateMetricQuery(name string, tags []string) (string, error) {
	buf := &strings.Builder{}
	for _, tag := range tags {
		if !metricAndTagsRe.MatchString(tag) {
			return "", errUnsupportedTagName
		}
		buf.WriteString("x_" + tag + " text,")
	}
	return fmt.Sprintf(`CREATE TABLE simpletsdb_%s (timestamp bigint,%svalue double precision,UNIQUE(timestamp,value))`, name, buf.String()), nil
}

func createMetric(name string, tags []string) error {
	if name == "" {
		return errMetricRequired
	}
	if !metricAndTagsRe.MatchString(name) {
		return errUnsupportedMetricName
	}
	queryStr, err := generateMetricQuery(name, tags)
	if err != nil {
		return err
	}
	if _, err = session.Query(queryStr); err != nil {
		if err.Error() == fmt.Sprintf(`pq: relation "simpletsdb_%s" already exists`, name) {
			return errMetricExists
		}
		return err
	}
	return nil
}

func metricExists(name string) (bool, error) {
	if name == "" {
		return false, errMetricRequired
	}
	if !metricAndTagsRe.MatchString(name) {
		return false, errUnsupportedMetricName
	}
	var (
		name0 string
		found bool
		err   error
	)
	scanner, err := session.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")
	if err != nil {
		return false, err
	}
	for scanner.Next() {
		err = scanner.Scan(&name0)
		if err != nil {
			scanner.Close()
			return false, err
		}
		if name0 == "simpletsdb_"+name {
			found = true
		}
	}

	if err := scanner.Err(); err != nil {
		return false, err
	}

	return found, nil
}

func deleteMetric(name string) error {
	if name == "" {
		return errMetricRequired
	}
	if !metricAndTagsRe.MatchString(name) {
		return errUnsupportedMetricName
	}
	_, err := session.Query(fmt.Sprintf("DROP TABLE simpletsdb_%s", name))
	if err != nil {
		if err.Error() == fmt.Sprintf(`pq: table "simpletsdb_%s" does not exist`, name) {
			return errMetricDoesNotExist
		}
		return err
	}
	return nil
}

func generatePointInsertionStringsAndValues(queries []*insertPointQuery, firstMetric string) (string, string, []interface{}, error) {
	tagsStrBuilder, valuesStrBuilder := &strings.Builder{}, &strings.Builder{}
	values := []interface{}{}

	tagsOrder := []string{}

	for k, _ := range queries[0].Tags {
		if !metricAndTagsRe.MatchString(k) {
			return "", "", nil, errUnsupportedTagName
		}
		tagsOrder = append(tagsOrder, k)
		tagsStrBuilder.WriteString("x_")
		tagsStrBuilder.WriteString(k)
		tagsStrBuilder.WriteString(",")
	}

	var i = 1
	for z, query := range queries {
		if query.Metric != firstMetric {
			return "", "", nil, errSameMetricRequiredForInsert
		}
		if !metricAndTagsRe.MatchString(query.Metric) {
			return "", "", nil, errUnsupportedMetricName
		}
		if query.Point == nil {
			return "", "", nil, errPointRequiredForInsertQuery
		}
		values = append(values, query.Point.Timestamp)
		valuesStrBuilder.WriteString("($" + strconv.Itoa(i) + ",")
		i++
		for _, t := range tagsOrder {
			v := query.Tags[t]
			if !metricAndTagsRe.MatchString(v) {
				return "", "", nil, errUnsupportedTagValue
			}
			valuesStrBuilder.WriteString("$" + strconv.Itoa(i) + ",")
			i++
			values = append(values, v)
		}
		values = append(values, query.Point.Value)
		valuesStrBuilder.WriteString("$" + strconv.Itoa(i) + ")")
		if z+1 < len(queries) {
			valuesStrBuilder.WriteString(",")
		}
		i++
	}
	return tagsStrBuilder.String(), valuesStrBuilder.String(), values, nil
}

/*
Must all be the same metric name
Must not be duplicates of (timestamp, value)
*/
func insertPoints(queries0 []*insertPointQuery) error {
	if len(queries0) == 0 {
		return nil
	}
	// batch the queries 200 at a time to get around
	// max insert limit of postgres
	firstMetric := queries0[0].Metric
	for i := 0; i < len(queries0); i += insertBatchSize {
		queries := queries0[i:min1(i+insertBatchSize, len(queries0))]
		tagsStr, valuesStr, values, err := generatePointInsertionStringsAndValues(queries, firstMetric)
		if err != nil {
			return err
		}
		queryStr := fmt.Sprintf(`INSERT INTO simpletsdb_%s (timestamp,%svalue) VALUES %s`, firstMetric, tagsStr, valuesStr)
		if _, err := session.Exec(queryStr, values...); err != nil && err.Error() != fmt.Sprintf(errStringDuplicate, firstMetric) {
			if err.Error() == fmt.Sprintf(errPgTableNotExist, firstMetric) {
				return errMetricDoesNotExist
			}
			return err
		}
	}
	return nil
}

func generateTagsQueryString(tags map[string]string, queryVals []interface{}, argsCounter int) (string, []interface{}, int, error) {
	s := &strings.Builder{}
	for k, v := range tags {
		if !metricAndTagsRe.MatchString(k) {
			return "", nil, 0, errUnsupportedTagName
		}
		if !metricAndTagsRe.MatchString(v) {
			return "", nil, 0, errUnsupportedTagValue
		}
		s.WriteString(fmt.Sprintf(" AND x_%s = $%s", k, strconv.Itoa(argsCounter+1)))
		argsCounter++
		queryVals = append(queryVals, v)
	}
	return s.String(), queryVals, argsCounter - 1, nil
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
		query.Start, query.End,
	}

	if len(query.Tags) > 0 {
		tagStr, queryVals, _, err = generateTagsQueryString(query.Tags, queryVals, 2)
		if err != nil {
			return nil, err
		}
	}

	queryStr := fmt.Sprintf(`SELECT timestamp, value FROM simpletsdb_%s WHERE timestamp >= $1 AND timestamp <= $2%s ORDER BY timestamp ASC%s`, query.Metric, tagStr, limitStr)

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
		query.Start, query.End,
	}

	if len(query.Tags) > 0 {
		tagStr, queryVals, _, err = generateTagsQueryString(query.Tags, queryVals, 2)
		if err != nil {
			return err
		}
	}

	queryStr := fmt.Sprintf(`DELETE FROM simpletsdb_%s WHERE timestamp >= $1 AND timestamp <= $2%s`, query.Metric, tagStr)

	if resp, err := session.Query(queryStr, queryVals...); err != nil {
		resp.Close()
		return nil
	} else {
		resp.Close()
	}
	return nil
}
