# SimpleTSDB

#### Create a metric

```curl
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"metric":"test0","tags":["id","type"]}' \
  http://127.0.0.1:8981/create_metric
```

#### Check if a metric exists

```curl
curl http://127.0.0.1:8981/metric_exists?metric=test0
```

Response:
```json
{"exists":true}
```

#### Insert a point

```curl
curl --header "Content-Type: application/x.simpletsdb.points" \
  --request POST \
  --data 'test0,id=1 type=high,123 946684800000000000' \
  http://127.0.0.1:8981/insert_points
```

#### Query points

```curl
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"metric":"test0","start":946684800000000000,"tags":{"id":"1"}}' \
  http://127.0.0.1:8981/query_points
```

Response:

```json
[{"value":123,"timestamp":946684800000000000}]
```