package main

func downsample(ds *downsampler) error {
	var (
		startTime             int64
		endTime               int64
		err                   error
		firstValueUpdate      bool
		checkFirstValueUpdate bool
	)
	if ds.LastDownsampledWindow == 0 {

		startTime, err = selectFirstTimestamp(ds.Metric, ds.Query.Tags)
		if err != nil {
			return err
		}

		endTime, err = selectLastTimestamp(ds.Metric, ds.Query.Tags)
		if err != nil {
			return err
		}
	} else {
		checkFirstValueUpdate = true
		startTime = ds.LastDownsampledWindow
		endTime, err = selectLastTimestamp(ds.Metric, ds.Query.Tags)
		if err != nil {
			return err
		}
	}
	pts, err := queryPoints(&pointsQuery{
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
		var i int
		if checkFirstValueUpdate && pts[0].Timestamp == ds.LastDownsampledWindow {
			err := updateFirstPointDownsample(ds.OutMetric, ds.Query.Tags, pts[0])
			if err != nil {
				return err
			}
			firstValueUpdate = true
			i++
		}
		if len(pts) > 1 {
			ipts := make([]*insertPointQuery, len(pts)-i)
			for ; i < len(pts); i++ {
				pt := pts[i]
				idx := i
				if firstValueUpdate {
					idx--
				}
				ipts[idx] = &insertPointQuery{
					Metric: ds.OutMetric,
					Tags:   ds.Query.Tags,
					Point:  pt,
				}
			}
			if err := insertPoints(ipts); err != nil {
				return err
			}
		}
		lastTimestamp := pts[len(pts)-1].Timestamp
		if err := updateLastDownsampledWindow(ds.ID, lastTimestamp); err != nil {
			return err
		}
		ds.LastDownsampledWindow = lastTimestamp
	}

	return nil
}
