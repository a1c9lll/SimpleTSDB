package datastore

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"simpletsdb/aggregators"
	"simpletsdb/core"
)

var (
	metricAndTagsRe          = regexp.MustCompile("^[a-zA-Z0-9_]+$")
	errUnsupportedMetricName = errors.New("valid characters for metrics are a-z, A-Z, 0-9, and _")
	errUnsupportedTagName    = errors.New("valid characters for tag names are a-z, A-Z, 0-9, and _")
	errMetricRequired        = errors.New("metric is required")
	errStartRequired         = errors.New("query start is required")
	errWindowRequiredForAvg  = errors.New("window must be set for average aggregator")
)

func generateMetricQuery(name string, tags []string) (string, error) {
	if !metricAndTagsRe.MatchString(name) {
		return "", errUnsupportedMetricName
	}
	buf := &strings.Builder{}
	for _, tag := range tags {
		if !metricAndTagsRe.MatchString(tag) {
			return "", errUnsupportedTagName
		}
		buf.WriteString("x_" + tag + " text,")
	}
	return fmt.Sprintf(`create table simpletsdb_%s (timestamp bigint,%svalue double precision,PRIMARY KEY(timestamp,value))`, name, buf.String()), nil
}

func CreateMetric(name string, tags []string) error {
	queryStr, err := generateMetricQuery(name, tags)
	if err != nil {
		return err
	}
	if _, err = session.Query(queryStr); err != nil {
		return err
	}
	return nil
}

func MetricExists(name string) (bool, error) {
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

func generatePointInsertionStringsAndValues(query *core.InsertPointsQuery) (string, string, []interface{}, error) {
	tagsStrBuilder, valuesStrBuilder := &strings.Builder{}, &strings.Builder{}
	values := []interface{}{query.Point.Timestamp}

	var i = 2
	for k, v := range query.Tags {
		if !metricAndTagsRe.MatchString(k) {
			return "", "", nil, errUnsupportedTagName
		}
		tagsStrBuilder.WriteString("x_")
		tagsStrBuilder.WriteString(k)
		tagsStrBuilder.WriteString(",")
		valuesStrBuilder.WriteString("$" + strconv.Itoa(i) + ",")
		i++
		values = append(values, v)
	}
	values = append(values, query.Point.Value)

	return tagsStrBuilder.String(), valuesStrBuilder.String(), values, nil
}

func generateTagsQueryString(tags map[string]string, queryVals []interface{}, argsCounter int) (string, []interface{}, int, error) {
	s := &strings.Builder{}
	for k, v := range tags {
		if !metricAndTagsRe.MatchString(k) {
			return "", nil, 0, errUnsupportedTagName
		}
		s.WriteString(fmt.Sprintf(" AND x_%s = $%s", k, strconv.Itoa(argsCounter+1)))
		argsCounter++
		queryVals = append(queryVals, v)
	}
	return s.String(), queryVals, argsCounter - 1, nil
}

func InsertPoint(query *core.InsertPointsQuery) error {
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
	queryStr := fmt.Sprintf(`insert into simpletsdb_%s (timestamp,%svalue) values ($1,%s$%d)`, query.Metric, tagsStr, valuesStr, len(values))
	if _, err = session.Query(queryStr, values...); err != nil && err.Error() != fmt.Sprintf(`pq: duplicate key value violates unique constraint "simpletsdb_%s_pkey"`, query.Metric) {
		return err
	}

	return nil
}

func QueryPoints(query *core.PointsQuery) ([]*core.Point, error) {
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
		points    []*core.Point
	)
	if err != nil {
		return nil, err
	}
	for scanner.Next() {
		err := scanner.Scan(&timestamp, &value)
		if err != nil {
			return nil, err
		}
		points = append(points, &core.Point{
			Value:     value,
			Timestamp: timestamp,
		})
	}

	var (
		windowApplied bool
	)

	if query.Window != nil {
		points, err = aggregators.Window(query.Start, query.End, query.Window, points)
		if err != nil {
			return nil, err
		}
		windowApplied = true
	}

	for _, aggregator := range query.Aggregators {
		switch aggregator.Name {
		case "average":
			if !windowApplied {
				return nil, errWindowRequiredForAvg
			}
			points = aggregators.Average(points)
		}
	}

	return points, nil
}
