package main

type point struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Window    int64   `json:"window,omitempty"`
	Null      bool    `json:"-"`
}

type insertPointQuery struct {
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
	Point  *point            `json:"point"`
}

type deletePointsQuery struct {
	Metric string            `json:"metric"`
	Start  int64             `json:"start"`
	End    int64             `json:"end"`
	Tags   map[string]string `json:"tags"`
}

type aggregatorQuery struct {
	Name    string                 `json:"name"`
	Options map[string]interface{} `json:"options"`
}

type pointsQuery struct {
	Metric      string                 `json:"metric"`
	Start       int64                  `json:"start"`
	End         int64                  `json:"end"`
	N           int64                  `json:"n"`
	Tags        map[string]string      `json:"tags"`
	Window      map[string]interface{} `json:"window"`
	Aggregators []*aggregatorQuery     `json:"aggregators"`
}

type serverError struct {
	Error string `json:"error"`
}

type metricExistsResponse struct {
	Exists bool `json:"exists"`
}

type createMetricRequest struct {
	Metric string   `json:"metric"`
	Tags   []string `json:"tags"`
}

type deleteMetricRequest struct {
	Metric string `json:"metric"`
}
