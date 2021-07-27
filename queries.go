package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	metricAndTagsRe          = regexp.MustCompile(`^[a-zA-Z0-9_\-.]+$`)
	errUnsupportedMetricName = errors.New("valid characters for metrics are a-z, A-Z, 0-9, -, ., and _")
	errUnsupportedTagName    = errors.New("valid characters for tag names are a-z, A-Z, 0-9, -, ., and _")
	errUnsupportedTagValue   = errors.New("valid characters for tag values are a-z, A-Z, 0-9, -, ., and _")
	errMetricRequired        = errors.New("metric is required")
	errMetricExists          = errors.New("metric already exists")
	errMetricDoesNotExist    = errors.New("metric does not exist")
	errStartRequired         = errors.New("query start is required")
	errEndRequired           = errors.New("query end is required")
	errStringDuplicate       = `pq: duplicate key value violates unique constraint "simpletsdb_%s_timestamp_value_key"`
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

func CreateMetric(name string, tags []string) error {
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

func MetricExists(name string) (bool, error) {
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

func DeleteMetric(name string) error {
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

func generatePointInsertionStringsAndValues(query *InsertPointQuery) (string, string, []interface{}, error) {
	tagsStrBuilder, valuesStrBuilder := &strings.Builder{}, &strings.Builder{}
	values := []interface{}{query.Point.Timestamp}

	var i = 2
	for k, v := range query.Tags {
		if !metricAndTagsRe.MatchString(k) {
			return "", "", nil, errUnsupportedTagName
		}
		if !metricAndTagsRe.MatchString(v) {
			return "", "", nil, errUnsupportedTagValue
		}
		tagsStrBuilder.WriteString("x_")
		tagsStrBuilder.WriteString(k)
		tagsStrBuilder.WriteString(",")
		valuesStrBuilder.WriteString("$" + strconv.Itoa(i) + ",")
		i++
		values = append(values, v)
	}
	if query.Point.Null {
		values = append(values, nil)
	} else {
		values = append(values, query.Point.Value)
	}
	return tagsStrBuilder.String(), valuesStrBuilder.String(), values, nil
}

func InsertPoint(query *InsertPointQuery) error {
	if query.Metric == "" {
		return errMetricRequired
	}
	if !metricAndTagsRe.MatchString(query.Metric) {
		return errUnsupportedMetricName
	}
	tagsStr, valuesStr, values, err := generatePointInsertionStringsAndValues(query)
	if err != nil {
		return err
	}
	queryStr := fmt.Sprintf(`INSERT INTO simpletsdb_%s (timestamp,%svalue) VALUES ($1,%s$%d)`, query.Metric, tagsStr, valuesStr, len(values))
	if _, err := session.Query(queryStr, values...); err != nil && err.Error() != fmt.Sprintf(errStringDuplicate, query.Metric) {
		return err
	}

	return nil
}

// This function just calls single inserts for each point.
// TODO: Batch the inserts if possible. Although it may be
//       difficult to keep the insert order with multiple different
//       metrics being inserted.
func InsertPoints(queries []*InsertPointQuery) error {
	ctx := context.Background()
	tx, err := session.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, query := range queries {
		if query.Metric == "" {
			tx.Rollback()
			return errMetricRequired
		}
		if !metricAndTagsRe.MatchString(query.Metric) {
			tx.Rollback()
			return errUnsupportedMetricName
		}
		tagsStr, valuesStr, values, err := generatePointInsertionStringsAndValues(query)
		if err != nil {
			tx.Rollback()
			return err
		}
		var (
			resp *sql.Rows
		)
		queryStr := fmt.Sprintf(`INSERT INTO simpletsdb_%s (timestamp,%svalue) VALUES ($1,%s$%d)`, query.Metric, tagsStr, valuesStr, len(values))
		if resp, err = tx.QueryContext(ctx, queryStr, values...); err != nil && err.Error() != fmt.Sprintf(errStringDuplicate, query.Metric) {
			tx.Rollback()
			resp.Close()
			return err
		} else {
			resp.Close()
		}
	}
	if err := tx.Commit(); err != nil {
		return err
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

func QueryPoints(query *PointsQuery) ([]*Point, error) {
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
		value     interface{}
		timestamp int64
		points    []*Point
	)
	if err != nil {
		scanner.Close()
		return nil, err
	}
	for scanner.Next() {
		err := scanner.Scan(&timestamp, &value)
		if err != nil {
			scanner.Close()
			return nil, err
		}
		if value == nil {
			points = append(points, &Point{
				Value:     0,
				Timestamp: timestamp,
				Null:      true,
			})
		} else {
			points = append(points, &Point{
				Value:     value.(float64),
				Timestamp: timestamp,
			})
		}
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
		points, err = Window(query.Start, query.End, query.Window, points)
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

func DeletePoints(query *DeletePointsQuery) error {
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
