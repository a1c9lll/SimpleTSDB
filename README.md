# SimpleTSDB

## Installation

SimpleTSDB uses PostgreSQL, so you will need to install and start PostgreSQL. After that download a tarball from the releases.

You will need to copy the config sample to somewhere you want to keep it.

```bash
cp config.sample config
```

Then just run the database:

```bash
./simpletsdb -config=/path/to/config
```

If all goes well you will see:

```
INFO[0000] Starting SimpleTSDB
INFO[0000] Connected to database [simpletsdb] at 127.0.0.1:5432
INFO[0000] Initializing server at 127.0.0.1:8981
```

## APIs

For now there is only a Nodejs API found [here](https://github.com/a1c9lll/node-simpletsdb).


## Using curl to test the database

#### Insert a point

```bash
curl --header "Content-Type: application/x.simpletsdb.points" \
  --request POST \
  --data 'test0,id=1 type=high,123 946684800000000000' \
  http://127.0.0.1:8981/insert_points
```

#### Query points

```bash
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"metric":"test0","start":946684800000000000,"tags":{"id":"1"}}' \
  http://127.0.0.1:8981/query_points
```

Response:

```json
[{"value":123,"timestamp":946684800000000000}]
```