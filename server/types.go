package server

type ServerError struct {
	Error string `json:"error"`
}

type MetricExistsResponse struct {
	Exists bool `json:"exists"`
}

type CreateMetricRequest struct {
	Metric string   `json:"metric"`
	Tags   []string `json:"tags"`
}

type DeleteMetricRequest struct {
	Metric string `json:"metric"`
}
