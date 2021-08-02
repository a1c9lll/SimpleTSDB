package main

func downsample(db *dbConn, ds *downsampler) error {
	var (
		startTime             int64
		endTime               int64
		err                   error
		checkFirstValueUpdate bool
	)
	if ds.LastDownsampledWindow == 0 {
		startTime, err = selectFirstTimestamp(db, ds.Metric, ds.Query.Tags)
		if err != nil && err.Error() == "sql: no rows in result set" {
			return nil
		} else if err != nil {
			return err
		}

		endTime, err = selectLastTimestamp(db, ds.Metric, ds.Query.Tags)
		if err != nil && err.Error() == "sql: no rows in result set" {
			return nil
		} else if err != nil {
			return err
		}
	} else {
		checkFirstValueUpdate = true
		startTime = ds.LastDownsampledWindow
		endTime, err = selectLastTimestamp(db, ds.Metric, ds.Query.Tags)
		if err != nil && err.Error() == "sql: no rows in result set" {
			return nil
		} else if err != nil {
			return err
		}
	}
	pts, err := queryPoints(db, &pointsQuery{
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
		if checkFirstValueUpdate && pts[0].Timestamp == ds.LastDownsampledWindow {
			err := updateFirstPointDownsample(db, ds.OutMetric, ds.Query.Tags, pts[0])
			if err != nil {
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
			if err := insertPoints(db, ipts); err != nil {
				return err
			}

			lastTimestamp := pts[len(pts)-1].Timestamp
			if err := updateLastDownsampledWindow(db, ds.ID, lastTimestamp); err != nil {
				return err
			}
			ds.LastDownsampledWindow = lastTimestamp
		}
	}

	return nil
}
