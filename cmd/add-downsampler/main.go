package main

/*
Example JSON input file:
{
  "metric": "price",
  "outMetric": "price_high_2361_15m",
  "runEvery": "1m",
  "query": {
    "tags": {
      "id": "2361",
      "type": "high"
    },
    "window": {
      "every": "15m"
    },
    "aggregators": [
      {
        "name": "mean"
      }
    ]
  }
}
*/
import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var (
	client *http.Client
)

func main() {
	host := flag.String("host", "127.0.0.1", "Host where SimpleTSDB is running")
	port := flag.Int("port", 8981, "Port where SimpleTSDB is running")
	fileName := flag.String("file", "", "File to upload to SimpleTSDB")
	flag.Parse()

	if *fileName == "" {
		log.Fatal("file argument is required")
	}

	file, err := os.Open(*fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	bs, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}

	buf := bytes.NewBuffer(bs)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%d/add_downsampler", *host, *port), buf)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Error body: %s\n", string(respBody))
		log.Printf("Error code: %d\n", resp.StatusCode)
	}
}

func init() {
	client = &http.Client{}
}
