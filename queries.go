package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	tagsIndexMap                           = map[string]bool{}
	tagsIndexMapMutex                      = &sync.Mutex{}
	createIndexMutex                       = &sync.Mutex{}
	metricAndTagsRe                        = regexp.MustCompile(`^[a-zA-Z0-9_\-.]+$`)
	insertBatchSize                        = 200
	errUnsupportedMetricName               = errors.New("valid characters for metrics are [a-zA-Z0-9\\-._]")
	errUnsupportedOutMetricName            = errors.New("valid characters for out metrics are [a-zA-Z0-9\\-._]")
	errUnsupportedTagName                  = errors.New("valid characters for tag names are [a-zA-Z0-9\\-._]")
	errUnsupportedTagValue                 = errors.New("valid characters for tag values are [a-zA-Z0-9\\-._]")
	errMetricRequired                      = errors.New("metric is required")
	errMetricDoesNotExist                  = errors.New("metric does not exist")
	errStartRequired                       = errors.New("query start is required")
	errEndRequired                         = errors.New("query end is required")
	errPointRequiredForInsertQuery         = errors.New("point required for insert query")
	errWindowRequiredForDownsampler        = errors.New("window required for downsampler")
	errRunEveryRequiredForDownsampler      = errors.New("run every required for downsampler")
	errOutMetricRequired                   = errors.New("out metric required")
	errQueryRequiredForDownsampler         = errors.New("query required for downsampler")
	errAggregatorsRequiredForDownsampler   = errors.New("aggregators option required for downsampler")
	errOneAggregatorRequiredForDownsampler = errors.New("at least one aggregator must be used in downsampler spec")
)

func databaseExists(name string) (bool, error) {
	var (
		name0 string
		found bool
		err   error
	)
	scanner, err := session.Query(fmt.Sprintf("SELECT datname FROM pg_database WHERE datname='%s';", name))
	if err != nil {
		return false, err
	}

	defer scanner.Close()

	for scanner.Next() {
		err = scanner.Scan(&name0)
		if err != nil {
			scanner.Close()
			return false, err
		}
		if name0 == name {
			found = true
		}
	}

	if err := scanner.Err(); err != nil {
		return false, err
	}

	return found, nil
}

func tableExists(name string) (bool, error) {
	var (
		name0 string
		found bool
		err   error
	)
	scanner, err := session.Query(fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name='%s';", name))
	if err != nil {
		return false, err
	}

	defer scanner.Close()

	for scanner.Next() {
		err = scanner.Scan(&name0)
		if err != nil {
			scanner.Close()
			return false, err
		}
		if name0 == name {
			found = true
		}
	}

	if err := scanner.Err(); err != nil {
		return false, err
	}

	return found, nil
}

func selectDownsamplers() ([]*downsampler, error) {
	query := fmt.Sprintf("SELECT id,metric,out_metric,run_every,last_downsampled_window,query FROM %s", downsamplersTable)
	scanner, err := session.Query(query)
	if err != nil {
		return nil, err
	}
	defer scanner.Close()
	var (
		downsamplers0         []*downsampler
		queryJSON             string
		runEvery              int64
		lastDownsampledWindow interface{}
	)
	for scanner.Next() {
		ds := &downsampler{
			Deleted: &AtomicBool{},
		}
		err := scanner.Scan(
			&ds.ID,
			&ds.Metric,
			&ds.OutMetric,
			&runEvery,
			&lastDownsampledWindow,
			&queryJSON,
		)
		if err != nil {
			return nil, err
		}
		if lastDownsampledWindow != nil {
			switch v := lastDownsampledWindow.(type) {
			case int:
				ds.LastDownsampledWindow = int64(v)
			case int32:
				ds.LastDownsampledWindow = int64(v)
			case int64:
				ds.LastDownsampledWindow = v
			default:
				return nil, errors.New("incorrect type for lastDownsampledWindow")
			}
		}
		ds.RunEvery = time.Duration(runEvery).String()
		ds.RunEveryDur = time.Duration(runEvery)

		ds.Query = &downsampleQuery{}
		err = json.Unmarshal([]byte(queryJSON), &ds.Query)
		if err != nil {
			return nil, err
		}
		downsamplers0 = append(downsamplers0, ds)
	}
	return downsamplers0, nil
}

func createIndex(tags []string) error {
	indexNameStr := &strings.Builder{}
	indexSchemaStr := &strings.Builder{}

	for _, t := range tags {
		indexNameStr.WriteString(t)
		indexNameStr.WriteRune('_')
		indexSchemaStr.WriteString("(tags->>'")
		indexSchemaStr.WriteString(t)
		indexSchemaStr.WriteString("')")
		indexSchemaStr.WriteRune(',')
	}
	indexNameStr.WriteString("timestamp")
	indexSchemaStr.WriteString("timestamp")

	t0 := time.Now()
	query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_%s ON %s(%s)", metricsTable, indexNameStr.String(), metricsTable, indexSchemaStr.String())
	_, err := session.Exec(query)
	if err != nil {
		return err
	}
	log.Infof("create index %v took %s", indexNameStr.String(), time.Since(t0))
	return nil
}

func createIndices() {
	createIndexMutex.Lock()
	tags := []string{}

	tagsIndexMapMutex.Lock()
	for k, v := range tagsIndexMap {
		if !v {
			tags = append(tags, k)
		}
	}
	tagsIndexMapMutex.Unlock()

	if len(tags) == 0 {
		return
	}

	sort.Strings(tags)

	err := createIndex(tags)
	if err != nil {
		log.Error(err)
		return
	}

	tagsIndexMapMutex.Lock()
	for _, s := range tags {
		tagsIndexMap[s] = true
	}
	tagsIndexMapMutex.Unlock()
	createIndexMutex.Unlock()
}

func generateInsertStringsAndValues(queries []*insertPointQuery) (string, []interface{}, error) {
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
		if query.Point.Null {
			values = append(values, nil)
		} else {
			values = append(values, query.Point.Value)
		}
		// check if any of the tags have not been checked for indexing
		tagsIndexMapMutex.Lock()
		for k := range query.Tags {
			if _, ok := tagsIndexMap[k]; !ok {
				tagsIndexMap[k] = false
			}
		}
		tagsIndexMapMutex.Unlock()

		bs, err := json.Marshal(query.Tags)
		if err != nil {
			return "", nil, err
		}
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
	// batch the queries insertBatchSize at a time to get around
	// max insert limit of postgres
	for i := 0; i < len(queries0); i += insertBatchSize {
		queries := queries0[i:min1(i+insertBatchSize, len(queries0))]
		valuesStr, values, err := generateInsertStringsAndValues(queries)
		if err != nil {
			return err
		}
		queryStr := fmt.Sprintf(`INSERT INTO %s (metric,timestamp,value,tags) VALUES %s ON CONFLICT DO NOTHING`, metricsTable, valuesStr)
		if _, err := session.Exec(queryStr, values...); err != nil { //&& err.Error() != fmt.Sprintf(errStringDuplicate, strings.ToLower(firstMetric)) {
			return err
		}
	}

	go createIndices()

	return nil
}

func generateTagsQueryStringAndValues(tags map[string]string, queryVals []interface{}) (string, []interface{}, error) {
	s := &strings.Builder{}
	argsCounter := len(queryVals)
	for k, v := range tags {
		if !metricAndTagsRe.MatchString(k) {
			return "", nil, errUnsupportedTagName
		}
		if !metricAndTagsRe.MatchString(v) {
			return "", nil, errUnsupportedTagValue
		}
		// check if tag is missing from index map, then schedule it for indexing if it is missing
		tagsIndexMapMutex.Lock()
		if _, ok := tagsIndexMap[k]; !ok {
			tagsIndexMap[k] = false
		}
		tagsIndexMapMutex.Unlock()

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
		tagStr, queryVals, err = generateTagsQueryStringAndValues(query.Tags, queryVals)
		if err != nil {
			return nil, err
		}
	}

	go createIndices()

	queryStr := fmt.Sprintf(`SELECT timestamp, value FROM %s WHERE metric = $1 AND timestamp >= $2 AND timestamp <= $3%s ORDER BY timestamp ASC%s`, metricsTable, tagStr, limitStr)

	scanner, err := session.Query(queryStr, queryVals...)
	if err != nil {
		return nil, err
	}

	var (
		val    interface{}
		points []*point
	)
	for scanner.Next() {
		pt := &point{}
		err := scanner.Scan(&pt.Timestamp, &val)
		if err != nil {
			scanner.Close()
			return nil, err
		}
		if val == nil {
			pt.Null = true
		} else {
			switch v := val.(type) {
			case int:
				pt.Value = float64(v)
			case int32:
				pt.Value = float64(v)
			case int64:
				pt.Value = float64(v)
			case float32:
				pt.Value = float64(v)
			case float64:
				pt.Value = v
			default:
				return nil, errors.New("incorrect type for lastDownsampledWindow")
			}
		}
		points = append(points, pt)
	}

	scanner.Close()

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

	if len(points) == 0 {
		return points, nil
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
		tagStr, queryVals, err = generateTagsQueryStringAndValues(query.Tags, queryVals)
		if err != nil {
			return err
		}
	}

	queryStr := fmt.Sprintf(`DELETE FROM %s WHERE metric = $1 AND timestamp >= $2 AND timestamp <= $3%s`, metricsTable, tagStr)

	if _, err := session.Exec(queryStr, queryVals...); err != nil {
		return err
	}
	return nil
}

func addDownsampler(ds *downsampler) error {
	if ds.Metric == "" {
		return errMetricRequired
	}
	if ds.OutMetric == "" {
		return errOutMetricRequired
	}
	if !metricAndTagsRe.MatchString(ds.Metric) {
		return errUnsupportedMetricName
	}
	if !metricAndTagsRe.MatchString(ds.OutMetric) {
		return errUnsupportedOutMetricName
	}
	if ds.Query == nil {
		return errQueryRequiredForDownsampler
	}
	if ds.Query.Window == nil {
		return errWindowRequiredForDownsampler
	}
	if _, ok := ds.Query.Window["every"]; !ok {
		return errEveryRequired
	}
	if ds.RunEvery == "" {
		return errRunEveryRequiredForDownsampler
	}
	if ds.Query.Aggregators == nil {
		return errAggregatorsRequiredForDownsampler
	}
	if len(ds.Query.Aggregators) == 0 {
		return errOneAggregatorRequiredForDownsampler
	}
	query := fmt.Sprintf("INSERT INTO %s (metric,out_metric,run_every,query) VALUES ($1,$2,$3,$4) RETURNING id", downsamplersTable)
	vals := []interface{}{
		ds.Metric,
		ds.OutMetric,
	}
	dur, err := time.ParseDuration(ds.RunEvery)
	if err != nil {
		return err
	}
	vals = append(vals, dur.Nanoseconds())
	ds.RunEveryDur = dur

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(ds.Query); err != nil {
		return err
	}
	vals = append(vals, buf.String())

	row := session.QueryRow(query, vals...)
	if row.Err() != nil {
		return err
	}
	if err = row.Scan(&ds.ID); err != nil {
		return err
	}

	downsamplers = append(downsamplers, ds)

	go waitDownsample(ds)

	return nil
}

func deleteDownsampler(ds *deleteDownsamplerRequest) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", downsamplersTable)
	vals := []interface{}{
		ds.ID,
	}
	_, err := session.Exec(query, vals...)
	if err != nil {
		return err
	}

	for i, d := range downsamplers {
		if d.ID == ds.ID {
			d.Deleted.Set(true)
			downsamplers = append(downsamplers[:i], downsamplers[i+1:]...)
			break
		}
	}

	return nil
}

func selectLastTimestamp(metric string, tags map[string]string) (int64, error) {
	vals := []interface{}{
		metric,
	}
	tagsStr, vals, err := generateTagsQueryStringAndValues(tags, vals)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("SELECT timestamp FROM %s WHERE metric = $1%s ORDER BY timestamp DESC LIMIT 1", metricsTable, tagsStr)
	scanner, err := session.Query(query, vals...)
	if err != nil {
		return 0, err
	}
	var (
		timestamp int64
		n         int
	)
	for scanner.Next() {
		n++
		err := scanner.Scan(&timestamp)
		if err != nil {
			scanner.Close()
			return 0, err
		}
	}
	if n == 0 {
		return 0, errors.New("no points in table")
	}

	scanner.Close()

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return timestamp, nil
}

func selectFirstTimestamp(metric string, tags map[string]string) (int64, error) {
	vals := []interface{}{
		metric,
	}
	tagsStr, vals, err := generateTagsQueryStringAndValues(tags, vals)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("SELECT timestamp FROM %s WHERE metric = $1%s ORDER BY timestamp ASC LIMIT 1", metricsTable, tagsStr)
	scanner, err := session.Query(query, vals...)
	if err != nil {
		return 0, err
	}
	var (
		timestamp int64
		n         int
	)
	for scanner.Next() {
		n++
		err := scanner.Scan(&timestamp)
		if err != nil {
			scanner.Close()
			return 0, err
		}
	}
	if n == 0 {
		return 0, errors.New("no points in table")
	}

	scanner.Close()

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return timestamp, nil
}

func updateFirstPointDownsample(metric string, tags map[string]string, point *point) error {
	vals := []interface{}{
		point.Value,
		metric,
		point.Timestamp,
	}
	tagsStr, vals, err := generateTagsQueryStringAndValues(tags, vals)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("UPDATE %s SET value = $1 WHERE metric = $2 AND timestamp = $3%s", metricsTable, tagsStr)
	if _, err := session.Exec(query, vals...); err != nil {
		return err
	}

	return nil
}

func updateLastDownsampledWindow(id int64, lastTimestamp int64) error {
	query := fmt.Sprintf("UPDATE %s SET last_downsampled_window = $1 WHERE id = $2", downsamplersTable)
	if _, err := session.Exec(query, []interface{}{
		lastTimestamp,
		id,
	}...); err != nil {
		return err
	}
	return nil
}
