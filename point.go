package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
)

var (
	errPointValueType = errors.New("point value must be null or number")
)

type Points []*Point

func (p Points) Len() int {
	return len(p)
}

func (p Points) Less(i, j int) bool {
	return p[i].Value < p[j].Value
}

func (p Points) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p *Point) MarshalJSON() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteString(`{"value":`)
	if p.Null {
		buf.WriteString("null")
	} else {
		buf.WriteString(strconv.FormatFloat(p.Value, 'f', -1, 64))
	}
	buf.WriteString(`,"timestamp":`)
	buf.WriteString(strconv.FormatInt(p.Timestamp, 10))
	if p.Window != 0 {
		buf.WriteString(`,"window":`)
		buf.WriteString(strconv.FormatInt(p.Window, 10))
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

type UnmarshallablePoint struct {
	Value     interface{} `json:"value"`
	Timestamp int64       `json:"timestamp"`
}

func (p *Point) UnmarshalJSON(bs []byte) error {
	pt := &UnmarshallablePoint{}
	err := json.Unmarshal(bs, pt)
	if err != nil {
		return err
	}
	if pt.Value == nil {
		p.Null = true
	} else {
		switch v := pt.Value.(type) {
		case int:
			p.Value = float64(v)
		case int32:
			p.Value = float64(v)
		case int64:
			p.Value = float64(v)
		case float32:
			p.Value = float64(v)
		case float64:
			p.Value = v
		default:
			return errPointValueType
		}
	}
	p.Timestamp = pt.Timestamp
	return nil
}
