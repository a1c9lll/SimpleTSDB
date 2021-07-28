# SimpleTSDB

## Installation

SimpleTSDB uses PostgreSQL, so you will need to install and start PostgreSQL.

Download a tarball from releases.

Then just run the database:

```bash
./simpletsdb
```

If all goes well you will see:

```
INFO[0000] Starting SimpleTSDB
INFO[0000] Connected to database [simpletsdb] at 127.0.0.1:5432
INFO[0000] Initializing server at 127.0.0.1:8981
```

## APIs

Nodejs API found [here](https://github.com/a1c9lll/node-simpletsdb).

Go API found [here](https://github.com/a1c9lll/go-simpletsdb).

## Windowing / Gap filling

SimpleTSDB can window points based on an interval. This groups points within windows of time and can then be used to aggregate the data. The `window` option has the following properties:

```
{
  window: {
    every: <time interval>, // a string like `10s`. Valid time qualifiers are `ns`, `ms`, `s`, `m`, and `h`
    createEmpty: <boolean>  // creates null points for empty windows
  }
}
```

If the data hasn't been passed to a windowed aggregator (see below) then the points returned by `query_points` will have their `window` property set.

## Aggregators

#### Sum

###### Name: `sum`

###### Requires window: `yes`

###### Options:

None

#### Min

###### Name: `min`

###### Requires window: `yes`

###### Options:

None

#### Max

###### Name: `max`

###### Requires window: `yes`

###### Options:

None

#### Count

###### Name: `count`

###### Requires window: `yes`

###### Options:

|Option           |Type     |Description                                                     |
|:--------------- |:------- |:-------------------------------------------------------------- |
|`countNullPoints`|`boolean`|Whether to count null points inserted by the windowing function.|

#### First

###### Name: `first`

###### Requires window: `yes`

###### Options:

None

#### Last

###### Name: `last`

###### Requires window: `yes`

###### Options:

None

#### Mean

###### Name: `mean`

###### Requires window: `yes`

###### Options:

None

#### Median

###### Name: `median`

###### Requires window: `yes`

###### Options:

None

#### Mode

###### Name: `mode`

###### Requires window: `yes`

###### Options:

None

#### Standard Deviation

###### Name: `stddev`

###### Requires window: `yes`

###### Options:

|Option|Type    |Description                                                     |
|:---- |:------ |:-------------------------------------------------------------- |
|`mode`|`string`|Either `population` or `sample`. Defaults to `sample`.          |

#### Fill

Fills null points, especially from the `window` function when `createEmpty` is true.

###### Name: `fill`

###### Requires window: `no`

###### Options:

|Option       |Type     |Description                                                     |
|:----------- |:------- |:-------------------------------------------------------------- |
|`fillValue`  |`number` |_required_: The value to fill null points with.                 |
|`usePrevious`|`boolean`|_optional_: Fills null values with the previous value.          |

Note: `usePrevious` may not have a value for the first point. This is why fillValue is required even if this is set.