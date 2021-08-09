package main

import (
	"bytes"
	"database/sql"
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
	priorityCRUD                           = 999
	priorityDownsamplers                   = 0
	tagsIndexMap                           = map[string]bool{}
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

func databaseExists(name string, session *sql.DB) (bool, error) {
	var (
		name0 string
		found bool
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

	scanner.Close()

	if err := scanner.Err(); err != nil {
		return false, err
	}

	return found, err
}

func tableExists(db *dbConn, name string) (bool, error) {
	var (
		name0 string
		found bool
	)
	err := db.Query(priorityCRUD, func(session *sql.DB) error {
		scanner, err := session.Query(fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name='%s';", name))
		if err != nil {
			return err
		}

		defer scanner.Close()

		for scanner.Next() {
			err = scanner.Scan(&name0)
			if err != nil {
				scanner.Close()
				return err
			}
			if name0 == name {
				found = true
			}
		}

		scanner.Close()

		if err := scanner.Err(); err != nil {
			return err
		}

		return nil
	})

	return found, err
}

func selectDownsamplers(db *dbConn) ([]*downsampler, error) {
	var (
		downsamplers0 []*downsampler
	)
	err := db.Query(priorityCRUD, func(session *sql.DB) error {
		query := fmt.Sprintf("SELECT id,metric,out_metric,run_every,last_downsampled_window,query FROM %s", downsamplersTable)
		scanner, err := session.Query(query)
		if err != nil {
			return err
		}
		defer scanner.Close()
		var (
			queryJSON             string
			runEvery              int64
			lastDownsampledWindow interface{}
		)
		for scanner.Next() {
			ds := &downsampler{}
			err := scanner.Scan(
				&ds.ID,
				&ds.Metric,
				&ds.OutMetric,
				&runEvery,
				&lastDownsampledWindow,
				&queryJSON,
			)
			if err != nil {
				return err
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
					return errors.New("incorrect type for lastDownsampledWindow")
				}
			}
			ds.RunEvery = time.Duration(runEvery).String()
			ds.RunEveryDur = time.Duration(runEvery)

			ds.Query = &downsampleQuery{}
			err = json.Unmarshal([]byte(queryJSON), &ds.Query)
			if err != nil {
				return err
			}
			downsamplers0 = append(downsamplers0, ds)
		}
		return nil
	})
	return downsamplers0, err
}

func selectDownsamplersCount(db *dbConn) (int, error) {
	var currentCount int
	err := db.Query(priorityDownsamplers, func(db *sql.DB) error {
		query := fmt.Sprintf("SELECT worker_id_count FROM %s", metaTable)
		row := db.QueryRow(query)
		if err := row.Scan(&currentCount); err != nil {
			return err
		}
		return nil
	})
	return currentCount, err
}

func insertDownsamplersInitialCount(db *dbConn) error {
	err := db.Query(priorityDownsamplers, func(db *sql.DB) error {
		query := fmt.Sprintf("INSERT INTO %s (worker_id_count) VALUES ($1)", metaTable)
		_, err := db.Exec(query, 0)
		return err
	})
	return err
}

func generateTagsIndexString(ss []string) string {
	s := &strings.Builder{}
	for _, s0 := range ss {
		s.WriteString(s0)
		s.WriteString("_")
	}
	return s.String()
}

func createIndex(db *dbConn, tags []string) {
	s := generateTagsIndexString(tags)

	createIndexMutex.Lock()
	defer createIndexMutex.Unlock()

	if v, ok := tagsIndexMap[s]; ok {
		if v {
			return
		}
	}

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

	err := db.Query(priorityCRUD, func(session *sql.DB) error {
		t0 := time.Now()
		query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_%s ON %s(%s)", metricsTable, indexNameStr.String(), metricsTable, indexSchemaStr.String())
		_, err := session.Exec(query)
		if err != nil {
			return err
		} else {
			tagsIndexMap[s] = true
		}
		log.Infof("create index if not exists %v took %s", indexNameStr.String(), time.Since(t0))
		return nil
	})

	if err != nil {
		log.Errorf("createIndex: %s", err)
	}
}

func uniqueTags(haystack [][]string, needle []string) bool {
	m := map[string]struct{}{}
	for _, s := range haystack {
		m[generateTagsIndexString(s)] = struct{}{}
	}
	_, ok := m[generateTagsIndexString(needle)]

	return !ok
}

func generateInsertStringsAndValues(queries []*insertPointQuery) (string, []interface{}, [][]string, error) {
	valuesStrBuilder := &strings.Builder{}
	values := []interface{}{}
	uniqueTagCombinations := [][]string{}
	var i = 1
	for z, query := range queries {
		if !metricAndTagsRe.MatchString(query.Metric) {
			return "", nil, nil, errUnsupportedMetricName
		}
		if query.Point == nil {
			return "", nil, nil, errPointRequiredForInsertQuery
		}

		values = append(values, query.Metric)
		values = append(values, query.Point.Timestamp)
		if query.Point.Null {
			values = append(values, nil)
		} else {
			values = append(values, query.Point.Value)
		}

		bs, err := json.Marshal(query.Tags)
		if err != nil {
			return "", nil, nil, err
		}
		values = append(values, string(bs))

		valuesStrBuilder.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d)", i, i+1, i+2, i+3))
		if z+1 < len(queries) {
			valuesStrBuilder.WriteString(",")
		}
		i += 4

		// get unique combinations of tags for indexing
		tags := make([]string, len(query.Tags))

		x := 0
		for k := range query.Tags {
			tags[x] = k
			x++
		}

		sort.Strings(tags)
		if uniqueTags(uniqueTagCombinations, tags) {
			uniqueTagCombinations = append(uniqueTagCombinations, tags)
		}
	}
	return valuesStrBuilder.String(), values, uniqueTagCombinations, nil
}

func insertPoints(db *dbConn, queries0 []*insertPointQuery) error {
	if len(queries0) == 0 {
		return nil
	}
	// batch the queries insertBatchSize at a time to get around
	// max insert limit of postgres
	for i := 0; i < len(queries0); i += insertBatchSize {
		queries := queries0[i:min1(i+insertBatchSize, len(queries0))]
		valuesStr, values, uniqueTagCombinations, err := generateInsertStringsAndValues(queries)
		if err != nil {
			return err
		}
		for _, s := range uniqueTagCombinations {
			if len(s) == 0 {
				continue
			}
			go createIndex(db, s)
		}
		err = db.Query(priorityCRUD, func(session *sql.DB) error {
			queryStr := fmt.Sprintf(`INSERT INTO %s (metric,timestamp,value,tags) VALUES %s ON CONFLICT DO NOTHING` /* */, metricsTable, valuesStr)
			if _, err := session.Exec(queryStr, values...); err != nil { //&& err.Error() != fmt.Sprintf(errStringDuplicate, strings.ToLower(firstMetric)) {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

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

		s.WriteString(fmt.Sprintf(" AND tags->>'%s' = $%s", k, strconv.Itoa(argsCounter+1)))
		argsCounter++
		queryVals = append(queryVals, v)
	}
	return s.String(), queryVals, nil
}

func queryPoints(db *dbConn, priority int, query *pointsQuery) ([]*point, error) {
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

		if len(query.Tags) > 0 {
			tags := make([]string, len(query.Tags))
			i := 0
			for k := range query.Tags {
				tags[i] = k
				i++
			}
			sort.Strings(tags)
			go createIndex(db, tags)
		}
	}

	var (
		points []*point
	)

	err = db.Query(priority, func(session *sql.DB) error {
		queryStr := fmt.Sprintf(`SELECT timestamp, value FROM %s WHERE metric = $1 AND timestamp >= $2 AND timestamp <= $3%s ORDER BY timestamp ASC%s`, metricsTable, tagStr, limitStr)

		scanner, err := session.Query(queryStr, queryVals...)
		if err != nil {
			return err
		}
		var (
			val interface{}
		)
		for scanner.Next() {
			pt := &point{}
			if err := scanner.Scan(&pt.Timestamp, &val); err != nil {
				scanner.Close()
				return err
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
					scanner.Close()
					return errors.New("incorrect type for lastDownsampledWindow")
				}
			}
			points = append(points, pt)
		}

		scanner.Close()

		if err := scanner.Err(); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
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

func deletePoints(db *dbConn, query *deletePointsQuery) error {
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
	err = db.Query(priorityCRUD, func(session *sql.DB) error {
		queryStr := fmt.Sprintf(`DELETE FROM %s WHERE metric = $1 AND timestamp >= $2 AND timestamp <= $3%s`, metricsTable, tagStr)

		if _, err := session.Exec(queryStr, queryVals...); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func addDownsampler(db *dbConn, downsamplersCountChan chan int, cancelDownsampleWait []chan struct{}, ds *downsampler) error {
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
	query := fmt.Sprintf("INSERT INTO %s (metric,out_metric,time_update_at,run_every,query,worker_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id", downsamplersTable)
	vals := []interface{}{
		ds.Metric,
		ds.OutMetric,
		0,
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
	t9 := time.Now()
	workerID := <-downsamplersCountChan
	fmt.Println("downsample count chan time=", time.Since(t9))
	vals = append(vals, buf.String())
	vals = append(vals, workerID)

	err = db.Query(priorityDownsamplers, func(session *sql.DB) error {
		t7 := time.Now()
		row := session.QueryRow(query, vals...)
		if err := row.Scan(&ds.ID); err != nil {
			return err
		}
		fmt.Println("downsample insert time=", time.Since(t7))
		if err := row.Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	t0 := time.Now()
	cancelDownsampleWait[workerID] <- struct{}{}
	fmt.Println("downsample cancel wait time = ", time.Since(t0))
	return nil
}

func generateDownsamplersQueryAndValues(dss []*downsampler, downsamplerCountChan chan int) (string, []interface{}, []int, error) {
	str := &strings.Builder{}
	values := []interface{}{}
	workerIDs := []int{}
	i := 1
	for z, ds := range dss {
		if ds.Metric == "" {
			return "", nil, nil, errMetricRequired
		}
		if ds.OutMetric == "" {
			return "", nil, nil, errOutMetricRequired
		}
		if !metricAndTagsRe.MatchString(ds.Metric) {
			return "", nil, nil, errUnsupportedMetricName
		}
		if !metricAndTagsRe.MatchString(ds.OutMetric) {
			return "", nil, nil, errUnsupportedOutMetricName
		}
		if ds.Query == nil {
			return "", nil, nil, errQueryRequiredForDownsampler
		}
		if ds.Query.Window == nil {
			return "", nil, nil, errWindowRequiredForDownsampler
		}
		if _, ok := ds.Query.Window["every"]; !ok {
			return "", nil, nil, errEveryRequired
		}
		if ds.RunEvery == "" {
			return "", nil, nil, errRunEveryRequiredForDownsampler
		}
		if ds.Query.Aggregators == nil {
			return "", nil, nil, errAggregatorsRequiredForDownsampler
		}
		if len(ds.Query.Aggregators) == 0 {
			return "", nil, nil, errOneAggregatorRequiredForDownsampler
		}
		//metric,out_metric,time_update_at,run_every,query,worker_id
		values = append(values, ds.Metric)
		values = append(values, ds.OutMetric)
		values = append(values, 0)
		dur, err := time.ParseDuration(ds.RunEvery)
		if err != nil {
			return "", nil, nil, err
		}
		values = append(values, dur.Nanoseconds())
		bs, err := json.Marshal(ds.Query)
		if err != nil {
			return "", nil, nil, err
		}
		values = append(values, string(bs))
		workerID := <-downsamplerCountChan
		values = append(values, workerID)
		if len(workerIDs) < downsamplerWorkerCount {
			workerIDs = append(workerIDs, workerID)
		}
		str.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", i, i+1, i+2, i+3, i+4, i+5))
		if z+1 < len(dss) {
			str.WriteString(",")
		}
		i += 6
	}

	return str.String(), values, workerIDs, nil
}

func addDownsamplers(db *dbConn, downsamplersCountChan chan int, cancelDownsampleWait []chan struct{}, downsamplers []*downsampler) error {
	insertStr, values, workerIDs, err := generateDownsamplersQueryAndValues(downsamplers, downsamplersCountChan)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO %s (metric,out_metric,time_update_at,run_every,query,worker_id) VALUES %s", downsamplersTable, insertStr)

	err = db.Query(priorityDownsamplers, func(session *sql.DB) error {
		t7 := time.Now()
		_, err = session.Exec(query, values...)
		fmt.Println("downsample insert time=", time.Since(t7))
		return err
	})
	if err != nil {
		return err
	}

	t0 := time.Now()
	for _, workerID := range workerIDs {
		cancelDownsampleWait[workerID] <- struct{}{}
	}
	fmt.Println("downsamples cancel wait time = ", time.Since(t0))
	return nil
}

func deleteDownsampler(db *dbConn, ds *deleteDownsamplerRequest) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", downsamplersTable)
	vals := []interface{}{
		ds.ID,
	}
	err := db.Query(priorityDownsamplers, func(session *sql.DB) error {
		_, err := session.Exec(query, vals...)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func selectLastTimestamp(db *dbConn, metric string, tags map[string]string) (int64, error) {
	vals := []interface{}{
		metric,
	}
	tagsStr, vals, err := generateTagsQueryStringAndValues(tags, vals)
	if err != nil {
		return 0, err
	}
	var (
		timestamp int64
	)
	err = db.Query(priorityDownsamplers, func(session *sql.DB) error {
		query := fmt.Sprintf("SELECT timestamp FROM %s WHERE metric = $1%s ORDER BY timestamp DESC LIMIT 1", metricsTable, tagsStr)
		scanner := session.QueryRow(query, vals...)
		if err := scanner.Scan(&timestamp); err != nil {
			return err
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		return nil
	})

	return timestamp, err
}

func selectFirstTimestamp(db *dbConn, metric string, tags map[string]string) (int64, error) {
	vals := []interface{}{
		metric,
	}
	tagsStr, vals, err := generateTagsQueryStringAndValues(tags, vals)
	if err != nil {
		return 0, err
	}
	var (
		timestamp int64
	)
	err = db.Query(priorityDownsamplers, func(session *sql.DB) error {
		query := fmt.Sprintf("SELECT timestamp FROM %s WHERE metric = $1%s ORDER BY timestamp ASC LIMIT 1", metricsTable, tagsStr)
		scanner := session.QueryRow(query, vals...)

		if err := scanner.Scan(&timestamp); err != nil {
			return err
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		return nil
	})

	return timestamp, err
}

func updateFirstPointDownsampleTx(tx *sql.Tx, metric string, tags map[string]string, point *point) error {
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
	if _, err := tx.Exec(query, vals...); err != nil {
		return err
	}
	return nil
}

func updateLastDownsampledWindowTx(tx *sql.Tx, id int64, lastTimestamp int64) error {
	vals := []interface{}{
		lastTimestamp,
		id,
	}
	query := fmt.Sprintf("UPDATE %s SET last_downsampled_window = $1 WHERE id = $2", downsamplersTable)
	if _, err := tx.Exec(query, vals...); err != nil {
		return err
	}
	return nil
}

func insertPointsTx(db *dbConn, tx *sql.Tx, queries0 []*insertPointQuery) error {
	if len(queries0) == 0 {
		return nil
	}
	// batch the queries insertBatchSize at a time to get around
	// max insert limit of postgres
	for i := 0; i < len(queries0); i += insertBatchSize {
		queries := queries0[i:min1(i+insertBatchSize, len(queries0))]
		valuesStr, values, uniqueTagCombinations, err := generateInsertStringsAndValues(queries)
		if err != nil {
			return err
		}
		for _, s := range uniqueTagCombinations {
			if len(s) == 0 {
				continue
			}
			go createIndex(db, s)
		}
		queryStr := fmt.Sprintf(`INSERT INTO %s (metric,timestamp,value,tags) VALUES %s ON CONFLICT DO NOTHING` /* */, metricsTable, valuesStr)
		if _, err := tx.Exec(queryStr, values...); err != nil { //&& err.Error() != fmt.Sprintf(errStringDuplicate, strings.ToLower(firstMetric)) {
			return err
		}
	}

	return nil
}
