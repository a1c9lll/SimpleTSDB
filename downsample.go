package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	errStrNoRowsInResultSet      = "sql: no rows in result set"
	errLastDownsampledWindowType = errors.New("incorrect type for lastDownsampledWindow")
)

func downsampleCountCoordinator(db *dbConn, downsamplersCount int, nextDownsamplerID chan int) {
	for {
		nextDownsamplerID <- downsamplersCount
		downsamplersCount++
		if downsamplersCount >= downsamplerWorkerCount {
			downsamplersCount = 0
		}
		err := db.Query(priorityDownsamplers, func(db *sql.DB) error {
			query := fmt.Sprintf("UPDATE %s SET worker_id_count = $1", metaTable)
			_, err := db.Exec(query, downsamplersCount)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Errorf("downsampleCoordinator: %s", err)
		}
	}
}

//select id, out_metric, last_updated, run_every,  (last_updated + run_every)::bigint - (extract(epoch from now())*1000000000)::bigint as time_until_update from simpletsdb_downsamplers;

func handleDownsamplers(db *dbConn, workerID int, cancelDownsampleWait chan struct{}) {
	for {
	start:
		var (
			timeUntilUpdate int64
			ds              = &downsampler{}
		)
		err := db.Query(priorityDownsamplers, func(db *sql.DB) error {
			var (
				queryJSON             string
				runEvery              int64
				lastDownsampledWindow interface{}
			)
			vals := []interface{}{
				time.Now().UnixNano(),
				workerID,
			}
			row := db.QueryRow("SELECT id,metric,out_metric,run_every,last_downsampled_window,query,(last_updated + run_every)::bigint - $1 AS time_until_update FROM simpletsdb_downsamplers WHERE worker_id = $2 ORDER BY time_until_update ASC LIMIT 1", vals...)
			err := row.Scan(
				&ds.ID,
				&ds.Metric,
				&ds.OutMetric,
				&runEvery,
				&lastDownsampledWindow,
				&queryJSON,
				&timeUntilUpdate,
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
					return errLastDownsampledWindowType
				}
			}
			ds.RunEvery = time.Duration(runEvery).String()
			ds.RunEveryDur = time.Duration(runEvery)

			ds.Query = &downsampleQuery{}
			err = json.Unmarshal([]byte(queryJSON), &ds.Query)
			if err != nil {
				return err
			}
			if err := row.Err(); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			if err.Error() == errStrNoRowsInResultSet {
				<-cancelDownsampleWait
				continue
			}
			panic(err)
		}
		if timeUntilUpdate > 0 {
			select {
			case <-cancelDownsampleWait:
				goto start
			case <-time.After(time.Duration(timeUntilUpdate)):
			}
		} else {
			select {
			case <-cancelDownsampleWait:
			default:
			}
		}
		t0 := time.Now()
		err = downsample(db, ds)
		if err != nil {
			/*log.Errorf("downsample error: %s", err)
			time.Sleep(30 * time.Second)
			goto retry
			*/
			panic(err)
		}

		err = db.Query(priorityDownsamplers, func(db *sql.DB) error {
			query := fmt.Sprintf("UPDATE %s SET last_updated = $1 WHERE id = $2", downsamplersTable)
			vals := []interface{}{
				time.Now().UnixNano(),
				ds.ID,
			}
			_, err := db.Exec(query, vals...)
			return err
		})
		if err != nil {
			panic(err)
		}
		t1 := time.Since(t0)
		log.Debugf("downsample %d took %dms", ds.ID, t1.Milliseconds())
	}
}

func downsample(db *dbConn, ds *downsampler) error {
	var (
		startTime             int64
		endTime               int64
		err                   error
		checkFirstValueUpdate bool
	)
	if ds.LastDownsampledWindow == 0 {
		startTime, err = selectFirstTimestamp(db, ds.Metric, ds.Query.Tags)
		if err != nil && err.Error() == errStrNoRowsInResultSet {
			return nil
		} else if err != nil {
			return err
		}

		endTime, err = selectLastTimestamp(db, ds.Metric, ds.Query.Tags)
		if err != nil && err.Error() == errStrNoRowsInResultSet {
			return nil
		} else if err != nil {
			return err
		}
	} else {
		checkFirstValueUpdate = true
		startTime = ds.LastDownsampledWindow
		endTime, err = selectLastTimestamp(db, ds.Metric, ds.Query.Tags)
		if err != nil && err.Error() == errStrNoRowsInResultSet {
			return nil
		} else if err != nil {
			return err
		}
	}
	pts, err := queryPoints(db, priorityDownsamplers, &pointsQuery{
		Metric:      ds.Metric,
		Start:       startTime,
		End:         endTime,
		Tags:        ds.Query.Tags,
		Window:      ds.Query.Window,
		Aggregators: ds.Query.Aggregators,
	})
	if err != nil {
		return err
	}

	if len(pts) > 0 {
		err = db.Query(priorityDownsamplers, func(db0 *sql.DB) error {
			ctx := context.Background()
			tx, err := db0.BeginTx(ctx, nil)

			defer func() {
				if err := tx.Commit(); err != nil {
					log.Errorf("downsample commit error: %s", err)
				}
			}()

			if err != nil {
				return err
			}
			if checkFirstValueUpdate && pts[0].Timestamp == ds.LastDownsampledWindow {
				err := updateFirstPointDownsampleTx(tx, ds.OutMetric, ds.Query.Tags, pts[0])
				if err != nil {
					if err0 := tx.Rollback(); err0 != nil {
						log.Errorf("updateFirstPointDownsampleTx rollback error: %s", err0)
					}
					return err
				}
				pts = pts[1:]
			}
			if len(pts) > 0 {
				ipts := []*insertPointQuery{}
				for i := 0; i < len(pts); i++ {
					pt := pts[i]

					ipts = append(ipts, &insertPointQuery{
						Metric: ds.OutMetric,
						Tags:   ds.Query.Tags,
						Point:  pt,
					})
				}
				if err := insertPointsTx(db, tx, ipts); err != nil {
					if err0 := tx.Rollback(); err0 != nil {
						log.Errorf("insertPointsTx rollback error: %s", err0)
					}
					return err
				}

				lastTimestamp := pts[len(pts)-1].Timestamp
				if err := updateLastDownsampledWindowTx(tx, ds.ID, lastTimestamp); err != nil {
					if err0 := tx.Rollback(); err0 != nil {
						log.Errorf("insertPointsTx rollback error: %s", err0)
					}
					return err
				}
			}
			return nil
		})

		return err
	}

	return nil
}
