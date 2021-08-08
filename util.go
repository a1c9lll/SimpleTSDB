package main

import (
	"bytes"
	"container/heap"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	configMatchRe     = regexp.MustCompile("[#].*\\n|\\s+\\n|\\S+[=]|.*\n")
	lineMatchRe       = regexp.MustCompile(`^\s*([a-zA-Z0-9\-_.]+)\s*,\s*((?:[a-zA-Z0-9\-_.]+\s*=\s*[a-zA-Z0-9\-_.]+\s*)*)\s*,\s*([+-]?([0-9]+([.][0-9]*)?|[.][0-9]+))\s+([0-9]+)\s*$`)
	errPointValueType = errors.New("point value must be null or number")
	errNoMatches      = errors.New("parse line: invalid line protocol syntax - no matches")
)

// time utils

func mustParseTime(t string) time.Time {
	t0, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		panic(err)
	}
	return t0
}

// min/max utils

func min1(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func min0(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max0(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// atomic bool utils

type AtomicBool struct {
	flag int32
}

func (b *AtomicBool) Set(value bool) {
	var i int32 = 0
	if value {
		i = 1
	}
	atomic.StoreInt32(&(b.flag), int32(i))
}

func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32(&(b.flag)) != 0
}

// db conn workers utils

type item struct {
	fn       func(*sql.DB) error
	done     chan error
	priority int
	index    int
}

type dbConn struct {
	queue *priorityQueue
	cond  *sync.Cond
}

func (db *dbConn) Query(priority int, fn func(*sql.DB) error) error {
	item := &item{
		fn:       fn,
		priority: priority,
		done:     make(chan error),
	}

	db.cond.L.Lock()
	heap.Push(db.queue, item)
	db.cond.Signal()
	db.cond.L.Unlock()

	err := <-item.done
	return err
}

type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// point/points utils

type points []*point

func (p points) Len() int {
	return len(p)
}

func (p points) Less(i, j int) bool {
	return p[i].Value < p[j].Value
}

func (p points) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p points) MarshalJSON() ([]byte, error) {
	if len(p) == 0 {
		return []byte(`[]`), nil
	}
	return json.Marshal([]*point(p))
}

func (p *point) MarshalJSON() ([]byte, error) {
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

func (p *point) UnmarshalJSON(bs []byte) error {
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

// line parser utils

func parseLineProtocol(line []byte) (*insertPointQuery, error) {
	strs := lineMatchRe.FindAllSubmatch(line, -1)
	if len(strs) != 1 {
		return nil, errNoMatches
	}

	match := strs[0]

	metric := string(match[1])

	tagsStrs := strings.Split(string(match[2]), " ")
	tags := map[string]string{}

	for _, s := range tagsStrs {
		if s == "" {
			break
		}
		s0 := strings.Split(s, "=")
		key := strings.Trim(s0[0], " \t")
		val := strings.Trim(s0[1], " \t")
		tags[key] = val
	}

	mVal := string(match[3])
	var (
		isNull bool
		value  float64
		err    error
	)
	if mVal == "null" {
		isNull = true
	} else {
		value, err = strconv.ParseFloat(mVal, 64)
		if err != nil {
			return nil, err
		}
	}

	timestamp, err := strconv.ParseInt(string(match[6]), 10, 64)
	if err != nil {
		return nil, err
	}

	return &insertPointQuery{
		Metric: metric,
		Tags:   tags,
		Point: &point{
			Value:     value,
			Timestamp: timestamp,
			Null:      isNull,
		},
	}, nil
}

// config utils

/*
The MIT License (MIT)

Copyright (c) 2016 Jim Lawless

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

func loadConfig(filename string, dest map[string]string) error {
	fi, err := os.Stat(filename)
	if err != nil {
		return err
	}
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	buff := make([]byte, fi.Size())
	f.Read(buff)
	f.Close()
	str := string(buff)
	if !strings.HasSuffix(str, "\n") {
		str += "\n"
	}
	s2 := configMatchRe.FindAllString(str, -1)

	for i := 0; i < len(s2); {
		if strings.HasPrefix(s2[i], "#") {
			i++
		} else if strings.HasSuffix(s2[i], "=") {
			key := strings.ToLower(s2[i])[0 : len(s2[i])-1]
			i++
			if strings.HasSuffix(s2[i], "\n") {
				val := strings.TrimSuffix(s2[i][0:len(s2[i])-1], "\r")
				i++
				dest[key] = val
			}
		} else if strings.Contains(" \t\r\n", s2[i][0:1]) {
			i++
		} else {
			return errors.New(`error in config near: "` + s2[i])
		}
	}
	return nil
}
