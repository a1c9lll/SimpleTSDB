package core

type Point struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Window    int64   `json:"window,omitempty"`
	Null      bool    `json:"-"`
}

type InsertPointQuery struct {
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
	Point  *Point            `json:"point"`
}

type AggregatorQuery struct {
	Name    string                 `json:"name"`
	Options map[string]interface{} `json:"options"`
}

type PointsQuery struct {
	Metric      string                 `json:"metric"`
	Start       int64                  `json:"start"`
	End         int64                  `json:"end"`
	N           int64                  `json:"n"`
	Tags        map[string]string      `json:"tags"`
	Window      map[string]interface{} `json:"window"`
	Aggregators []*AggregatorQuery     `json:"aggregators"`
}
