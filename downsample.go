package main

import (
	"context"
	"database/sql"

	log "github.com/sirupsen/logrus"
)

var (
	errStrNoRowsInResultSet = "sql: no rows in result set"
)

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

				if err := tx.Commit(); err != nil {
					return err
				}
			}
			return nil
		})

		return err
	}

	return nil
}
