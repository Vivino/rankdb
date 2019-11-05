# RankDB

[![Build Status](https://travis-ci.com/Vivino/rankdb.svg?branch=master)](https://travis-ci.com/Vivino/rankdb)


RankDB is a fast and scalable ranking system optimized for realtime ranking of various metrics.

The system will provide _ranking_ of elements based on a _score_. 
The ranking of the elements can be retrieved either by element IDs or retrieving ordered segment lists.  

# Features

* Fast updates with resulting ranking.
* Fast bulk ingest of new scores.
* Scalable to many millions of entries in a single ranking list.
* Stable sorting of items in list.
* Provide lookup by secondary index or rank with single read.
* Provides rank comparisons between a subset of all elements.
* Many independent lists per server instance.
* Crosslist requests based on custom metadata.
* Experimental JWT authentication. 


# Installing

All modes require a configuration file to be set up.

A good basis for cofiguration can be found in `conf/conf.stub.toml`. 
You can copy this to `conf/conf.toml` and adjust it to your needs. 

## From binaries

* Extract archive
* Go to the folder containing the files
* Copy the configuration as described above
* Execute `rankdb`

You can now open the documentation/sample UI on http://127.0.0.1:8080/doc

## Go

RankDB uses modules. If you install RankDB inside GOPATH, be sure to use `GO111MODULE=on`. 
This will ensure that dependencies are correctly fetched.
 
```
go get -u github.com/Vivino/rankdb/cmd/rankdb
```

* Go to the folder containing the files.
* Copy the configuration as described above.
* Execute `rankdb`.

You can now open the documentation/sample UI on http://127.0.0.1:8080/doc

## Docker

A Dockerfile is included. You can download the docker image from docker hub:

```
// TODO, something like:

docker pull vivino/rankdb
docker run -e "GOGC=25" -p 8080:8080 -v /mnt/data:/data -v /mnt/conf:/conf vivino/rankdb
```

By default the server will start on port 8080, which you can of course remap to a local port.

The following paths can/should be set:
 
 * `/conf` should contain a `conf.toml` file with your configuration.
 * `/data` should point to where you want the local DB to be stored if you use one.
 * `/jwtkeys` can be used to add jwt keys. Set `JwtKeyPath = "/jwtkeys"` in your config.  

## Sample Data

You can try out a test data set you can [download here](https://github.com/Vivino/rankdb/releases/tag/sample).

It will add some rather large tables to your database and you can test queries, etc. on that.

## Glossary

* Element: An entry in the ranked list.
* Element ID: An ID of an element. An element ID can only be present in the list once.
* Score: The value that determines the placement of an element in the ranked list.
* Segment: A segment defines a part of the complete sorted list. A segment will only contain elements that are within the minimum/maximum values of the Segment. Segments cannot overlap.
* Index: An index of elements sorted by their ID. The index points to which segment contains each object.
* Element Index: Ordered list containing all segments pre-sorted by Element ID.
* List: Complete structure containing Rank and Element Index. Updates and reads go through this.
* Metadata: Custom JSON that can be attached to elements and lists.

## Assumptions/Limitations

To make RankDB efficient, some limitations must be fulfilled:

* Your element IDs must be representable as 64 bit unsigned integers.
* The element scores must be representable as a sortable value; 64 bit score and 32 bit tiebreaker.

We provide a convenient reversible converter from 64 bit floats to 64 bit unsigned integers. 
See the `sortfloat` package.

With these limitations, it should be possible to use the RankDB server with your data.


## Memory Usage

For optimal performance RankDB tries to keep frequently updated data in memory, 
so configuring the server to match your RAM amount available is important. 

Use element metadata sparingly. Only use it for data you will actually need or consider keeping it separately.
While it may be convenient to include a lot of information, it will always be loaded alongside each element.

Keep segment split/merge size reasonable. If you have much metadata you might consider setting this lower than the default values.

Use the `load_index` only on selected lists. The segments (but not elements) of these lists are loaded on startup and always kept in memory. 

The `CacheEntries` in the configuration specifies how many segment elements to keep in memory. 
This can significantly speed up reading/updating often accessed data. 
Each entry in the cache contain all the elements of a segment. 
If a segment not present in the cache is not found, it will be loaded from storage.

The `LazySaver` will keep data to be saved in memory for a an amount of time. 
This can significantly reduce the number of writes to storage for frequently updated segments.

Two limits are set on this. When the number of stored item reaches `FlushAtItems`, 
it will begin to flush the oldest items to stay below this number.
When the number of items reaches `LimitItems`, writes will begin to block and writes will not 
complete until the server is below this number. This will affect performance of write endpoints. 
 
Tweak the `GOGC` environment variable. It is reasonable to reduce the default (100) to something between 25 and 50.

It is possible to limit the number of concurrent updates with `MaxUpdates`. 
This will be shared across all endpoints that updates and will block updates from queuing up in memory.
Read operations are not affected by this, but limiting the number of concurrent updates will help 
ensure that read operations can always complete in reasonable time.

Keep bulk updates at a reasonable size. 
While bulk operations are significantly faster than single operations, they can potentially use a lot of memory.
For a single bulk update operation all affected segments and indexes will be loaded so all updates can be applied at once.
So for really big updates this could become a problem for the server.  

## Multiserver setup

RankDB does not support multiple servers accessing the same data.

It is of course possible to set up multiple servers by doing manual sharding of lists and keeping specific lists on separate servers.

## Consistency

Do not use RankDB as the primary storage source. 

To keep up performance some data is kept in memory and not stored at once.
While a lot of measures have been put in place to prevent data loss, 
an unexpected server shutdown is likely to cause data inconsistencies. 

Design your system so you are able to repopulate data.
While there is functionality to repair inconsistent lists, 
the repaired list is likely to be missing fully updated information. 

While an element is being updated it might return inconsistent results
until the update has completed. Update functions will allow to return updated
data, so use that if this can cause problems.


# API

Defined in `api/design` using goa DSL. Generated swagger definitions can be found in `api/swagger`.

To view API documentation:

* Go to `api` folder.
* First time `go get -u ./...`.
* Execute `go build && api` to start server.
* Navigate to http://localhost:8080/doc 

Not all properties are shown in the UI. 

To view documentation using [Chrome Plugin](https://chrome.google.com/webstore/detail/swagger-ui-console/ljlmonadebogfjabhkppkoohjkjclfai):

* Install server as above.
* Open in UI http://localhost:8080/api/swagger/swagger.json 

## Basic API usage

### Create a list

Use `POST /lists` with payload:

```JSON
{
  "id": "highscore-list",
  "load_index": false,
  "merge_size": 500,
  "metadata": {
    "country": "dk",
    "game": "2"
  },
  "populate": [
    {
      "id": 101,
      "payload": {
        "country": "dk",
        "name": "Tom Kristensen"
      },
      "score": 500,
      "tie_breaker": 1000
    },
    {
      "id": 102,
      "payload": {
        "country": "uk",
        "name": "Anthony Davidson"
      },
      "score": 200,
      "tie_breaker": 2000
    }
  ],
  "set": "storage-set",
  "split_size": 2000
}
```

This will create a list called "highscore-list". See [documentation with the running server](http://127.0.0.1:8080/doc/#/lists/lists_create)

```JSON
{
  "avg_segment_elements": 2,
  "bottom_element": {
    "from_bottom": 0,
    "from_top": 1,
    "id": 102,
    "list_id": "highscore-list",
    "payload": {
      "country": "uk",
      "name": "Anthony Davidson"
    },
    "score": 200,
    "tie_breaker": 2000,
    "updated_at": "2019-07-22T13:00:47Z"
  },
  "cache_hits": 0,
  "cache_misses": 0,
  "cache_percent": 0,
  "elements": 2,
  "id": "highscore-list",
  "load_index": false,
  "merge_size": 500,
  "metadata": {
    "country": "dk",
    "game": "2"
  },
  "segments": 1,
  "set": "storage-set",
  "split_size": 2000,
  "top_element": {
    "from_bottom": 1,
    "from_top": 0,
    "id": 101,
    "list_id": "highscore-list",
    "payload": {
      "country": "dk",
      "name": "Tom Kristensen"
    },
    "score": 500,
    "tie_breaker": 1000,
    "updated_at": "2019-07-22T13:00:47Z"
  }
}
```

To get an element in the list, use `GET /lists/highscore-list/elements/101?range=5`. 
This will return the elements and up to 5 neighbors in each direction.

```JSON
{
  "from_bottom": 1,
  "from_top": 0,
  "id": 101,
  "list_id": "highscore-list",
  "neighbors": {
    "below": [
      {
        "from_bottom": 0,
        "from_top": 1,
        "id": 102,
        "list_id": "highscore-list",
        "payload": {
          "country": "uk",
          "name": "Anthony Davidson"
        },
        "score": 200,
        "tie_breaker": 2000,
        "updated_at": "2019-07-22T13:00:47Z"
      }
    ]
  },
  "payload": {
    "country": "dk",
    "name": "Tom Kristensen"
  },
  "score": 500,
  "tie_breaker": 1000,
  "updated_at": "2019-07-22T13:00:47Z"
}
```

There are functions to add, update, delete elements and multiple way of querying and get ranks.

There are operations that work across multiple lists. 
With these queries you can specify which lists to operate on, or you can give list metadata to match.

See the documentation for more details, or the section below which describes the technical details more.  

## Storage

An atomic key/value blob store must be provided. The storage layer should be able to replace data blobs atomically.

The storage layer is a relatively easy to implement and must simply be able to store blobs of bytes.

### Provided storage:

* Badger: Provides local file-based storage.
* BoltDB: Provides local file-based storage.
* Aerospike: Provides storage on Aerospike cluster.
* DynamoDB: Provides storage on DynamoDB. (experimental, not exposed)
* Memstore/NullStore: Provides storage for tests, etc.

### Storage helpers:

* LazySaver: Allows in-memory write caching, significantly reducing redundant writes.
* MaxSize: Allows splitting of elements if storage has an upper size limit on writes.
* Retry: Retries read/writes if underlying storage returns errors.
* Test: Allows to insert hooks for checking read, write and delete operations. Available only for development.


# List Management

Each ranking list operates independently from each other. 
However, for convenience there are functions that allow operations to be applied to several lists at once.
 
Lists are defined uniquely by a string ID, but each list can also have meta-data defined as `string -> string` key value pairs.  
It is possible to specify operations that will execute on specific meta-data values.

The server keeps track of available lists. Lists must be fast loading, or available on demand.

Lists can be created with optional content, or they can be cloned from other lists. 

# Single List Structure

## Elements

Each element in the ranking list contains the following information:

```Go
type Element struct {
	Score      uint64
	ElementID  uint64
	Payload    []byte
	TieBreaker uint32
	Updated    time.Time
}
```

`Score` is the score that determines the ranking in the list. 
In case of multiple similar scores, a `TieBreaker` can be supplied as a secondary sorting option to provide consistent sorting. 
Float64 values can be converted to sortable score, and can be reversed if sorted values are floating point values.

The `ElementID` provides an ID for the element. Each Element ID can only be present in a ranking list once.

An optional `Payload` can be set for an element, which will be returned along with results. 
The payload is returned untouched, and can optionally be updated along other updates. 

## Segments

The entire ranking list will be split into (sorted) list segments.
Two values "Split Size" and "Merge Size" are specified. 
When a segment size is greater than "Split size" it is split into two segments. 
When two adjacent segments have less than "Merged Size" elements combined they are joined.

Segment sizes should be...

* Small enough to quickly load/search all elements on a single update.
* Big enough to provide a significant speedup when doing search/aggregate calculations.

Suggested sizes could be in the range of 1000-2000 elements per segment.

A segment index will be created, so it will know the range of score values that is represented by each range. 
Segments are stored as sorted, non-overlapping elements for fast traversal, 
and is stored as a single blob for quick reload.

The server will keep the segment index in memory. 
Each segment index will contain this information about the range it represents:

```Go
type Segment struct {
	ID             uint64
	Min, Max       uint64
	MinTie, MaxTie uint32
	Elements       int
	Updated        time.Time
}
```

`ID` is a unique ID identifying the range. `Min`/`Max` describes the minimum and maximum value in the segment, 
along with tiebreakers. `Elements` represents the number of elements in the segment.

The first segment created will contain the range from `0` to `math.MaxUint64`, 
and when ranges split, the center values will determine the range of the two segments.

This structure allows for a fast linear search to identify the exact range needed to provide a specific rank, 
either by accumulating Elements (get rank X) or by checking min/max (is value inside range). 

The structure will be used for representing both the Rank Index and the Element Index. 
Updates will affect both indexes, but most operations will only require at most 2 ranges to be touched.

# Rank Index

![rank-index dot](https://user-images.githubusercontent.com/5663952/28068947-e302c4be-6647-11e7-9731-66d1310138b4.png)


# Element Index

![user-index dot](https://user-images.githubusercontent.com/5663952/28076555-64bee69c-665f-11e7-9eca-4d95e9f7e0ae.png)


# Operation Complexity

This describes expected complexity in terms of IO per operation. 
This is *excluding* any LRU/lazy write cache, which may void the need for certain read/writes, 
so should be considered worst case scenarios.

* EI = Element Index
* RI = Rank Index	
* $$ = Segment Size


| Operation                     | Parameter       | Read Ops                                       | Write Ops            | Notes                                                                     |
|-------------------------------|-----------------|------------------------------------------------|----------------------|---------------------------------------------------------------------------|
| Get element rank              | Element ID         | 1 EI, 1RI                                      | -                    |                                                                           |
| Get element at percentile     | Percentage      | 1RI                                            | -                    |                                                                           |
| Get element+surrounding at rank | Element ID, radius | 1 EI, 1RI                                      | -                    | +1 RI if radius goes into another segment.                                |
| Get element at rank              | Rank            | 1 RI                                           | -                    |                                                                           |
| Get elements in rank range       | From/To Rank    | 1 + FLOOR((To-From)/$$) RI                     | -                    |                                                                           |
| Elements with score              | Score           | 1 RI (+1 if score crosses into other segments) | -                    |                                                                           |
| Update Element Score             | Element ID, Score  | 1 EI, 2 RI                                     | 2 RI (old+new), 1 EI | If element remains in same segment only 1 RI op and no EI.            |
| Delete Element Score             | Element ID         | 1 EI, 1 RI                                     | 1 RI, 1 EI           |                                                                               |
| Bulk Update Score             | Element ID, Score  | 1 EI, 2 RI                                     | 2 RI (old+new), 1 EI | Segments affected by multiple elements will only need to be read/write once. |
| Bulk Create Table             | Element ID, Score  | -                                              | 2*len(elems) / $$    |                                                                           |
| Automatic Split               | -               | 1 RI, $$ EI                                    | 2 RI,  $$ EI            | Only unique EI segments will be written. One Segment is retained, so no EI update. |
| Automatic Merge               | -               | 2 RI, $$ EI                                    | 1 RI, $$ / 4 EI         | Only unique EI segments will be written. One Segment is retained, so no EI update. |
| Get elements below/above percentile | Percentage      | users/$$ RI                                    | -                    |                                                                           |

The segment lists for rank and elements are kept in memory, 
but dumped at a regular interval or when split/merges occur. 
Both of these indexes can be recreated by reading through the complete segment lists in case of corruption.

Only automatic Split/Join are complex operations, but the segment size keeps the operation impact to a fairly low one.

Segment splitting/joining is done by an asynchronous job

## Concurrency

The database is capable of concurrent operations. 
However be aware that results of concurrent operations on a list can yield unpredictable results.
If two updates are sent to the database, it is undefined which will be applied first. 


## Security

The RankDB API has optional [jwt](https://jwt.io/) security feature for restricting access to the database. 
If your use case is a service behind a firewall, 
you are free to skip securing your server more than you would otherwise so.

If you choose to keep security disabled, you can disregard the API security requirements.
Security is enabled by setting `JwtKeyPath` config value.

The API has basic access control based on scopes. 
Currently `api:read`, `api:update`, `api:delete` and `api:manage` are available as scopes.
In the API documentation it is stated what is required for each endpoint. 
A jwt token can contain several scopes, so `"api:read api:update"` will allow access to read and update API endpoints.

For read, update+delete you can add restrictions to only allow direct access to either specific lists or elements. 
This is done by adding custom fields to the jwt token claims. `"only_elements": "2,3,6"` will only allow reads/updates/deletes on the specified elements.
`"only_lists": "list1,list2,list5"` will only allow access to the specified lists. 
`"api:manager"` does not enforce these restrictions.

Note that some calls returns "neighbors" of the current ranked element. 
These elements are not checked for read access.

For testing, the `/jwt` endpoint can be used to generate tokens, 
you can however use your favorite [jwt library](https://jwt.io/#libraries). 

Only RSA Signatures (RSnnn) are supported. 
This allows you to generate custom keys from other servers, 
and this server will only need the public key to validate the request.

# Go Client

An autogenerated client for Go is provided in [`github.com/Vivino/rankdb/api/client`](https://godoc.org/github.com/Vivino/rankdb/api/client). 
You can choose to use this instead of regular HTTP calls.

The client provides structs with predefined data types. 
The client provides marshal/unmarshal functionality of request data.

An example of setting up the client with retry functionality can be seen in `docs/client_example.go`

With that setup a sample request will look like this:

```Go
func PrintPercentile(list string, pct, neighbors int) error {
	// Create a client
	c := Client("127:0.0.1:8080")
    
	// Perform the request
	resp, err = c.GetPercentileLists(ctx, client.GetPercentileListsPath(list), strconv.Itoa(pct), &neighbors)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return DecodeError(resp)
	}

	// Decode result
	res, err := c.DecodeRankdbElementFull(resp)
	if err != nil {
		return err
	}
	fmt.Println(res)
	return nil
}
```

Note that minor version upgrades may change client signatures. So upgrading from `v1.0.x` to `v1.1.0` may include changes to client signatures.  

# Backup

The server supports the following modes of backup. 

1) Backup to local file on the server. 
2) Backup to another RankDB server. 
3) Upload to S3 compatible storage.
4) Download to calling machine.
 

## Backup to local file

This will start an async job that will save the backup to a local path on the server.

The destination path can get sent and it is possible to filter which lists to back up.

```
$curl -X PUT "http://127.0.0.1:8080/xlist/backup" \
-H "Content-Type: application/json" \
-d "{ \"destination\": { \"path\": \"/tmp/backup.bin\", \"type\": \"file\" }, \"lists\": {}}"
```

Example response:
```JSON
{
  "callback_url": "/backup/c551KHYLi1UxjlbWQ4",
  "id": "c551KHYLi1UxjlbWQ4"
}
```

It is possible to query for the progress:

```
curl -X GET "http://127.0.0.1:8080/backup/c551KHYLi1UxjlbWQ4"
```
Which for example can return:
```JSON
{
  "cancelled": false,
  "done": true,
  "finished": "2019-05-31T14:44:50.316372113Z",
  "lists": 0,
  "size": 34,
  "started": "2019-05-31T14:44:50.315633403Z",
  "storage": "*file.File",
  "uri": "/backup/c551KHYLi1UxjlbWQ4"
}
```

A backup job can be cancelled:

```
curl -X DELETE "http://127.0.0.1:8080/backup/c551KHYLi1UxjlbWQ4"
```

## Backup to another server.

This will transfer contents of a server to another server.

```
curl -X PUT "http://127.0.0.1:8080/xlist/backup" \
-H "Content-Type: application/json" -d \
"{ \"destination\": { \"path\": \"10.0.0.1:8080\", \"type\": \"server\" }, \"lists\": {}}"
```

Will transfer all lists from `127.0.0.1:8080` to `10.0.0.1:8080`.

Status of the transfer can be queried in the same manner as above.

The receiving server should have a `ReadTimeout/WriteTimeout` to be able to process the entire set.
  

## Backup to S3

This will backup all content directly to S3.

```
curl -X PUT "http://127.0.0.1:8080/xlist/backup" \ 
-H "Content-Type: application/json" -d \
"{ \"destination\": { \"path\": \"s3://backup-bucket/path/file-backup.bin\", \"type\": \"s3\" }, \"lists\": {}}"
```

This will start the backup job directly to s3. 
To specify the destination, use the following syntax: `s3://{bucket}/{path+file}`. 
The same syntax can be used for restoring.

The credentials should be configured in the `[AWS]` section of the configuration:

```toml
[AWS]
Enabled = false

# Specify the region to use.
Region = ""

# URL to object storage service.
# Leave blank to use standard AWS endpoint.
S3Endpoint = ""

# Access keys can be specified here, or be picked up from environment:
# * Access Key ID: AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY
# * Secret Access Key: AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY
# If running on an EC2 instance credentials will also be attempted to be picked up there.
# These config values take priority.
AccessKey = ""
SecretKey = ""
```


## Download to calling machine

This will return the backup data to the caller. 

``` 
$ curl -J -L -O -X PUT "http://127.0.0.1:8080/xlist/backup"
 -H "Content-Type: application/json" \
 -d "{ \"destination\": { \"path\":\"\", \"type\": \"download\" }, \"lists\": { }}"
```

This will save the backup to the current directory with the ID of the backup. Alternatively `curl -D -o backup.bin -X PUT ....` can be used to save to a specific file. The ID of the backup will be returned.

## Recompressing

The data is lightly compressed `zstd` stream. It can be further re-compressed using the zstd commandline:

`zstd -c -d LaLh1KyaeUCu0WCaQ4.bin | zstd -T0 -19 -o 06-04-18-ranks-backup.zst`

This command will recompress to level 19. Typically this will result in a further 1.5x reduction of data size.
The recompressed stream can be used for restoring instead of the original.

This can of course also be done as part of the curl download:

```
curl -X PUT "http://127.0.0.1:8080/xlist/backup" \
-H "accept: application/octet-stream" -H "Content-Type: application/json" \
-d "{ \"destination\": { \"type\": \"download\" }, \"lists\": { \"match_metadata\": { \"country\": \"dk\" } }}" | zstd -d -c - | zstd -T0 -19 -o 06-04-18-ranks-backup.zst
```
Reduce `-19` if this is too slow.

# Restoring data

Restoring data is done by sending the binary data to the server:

```
$curl -i -T backup.bin -X POST "http://127.0.0.1:8080/xlist/restore"
HTTP/1.1 100 Continue

HTTP/1.1 200 OK
Content-Type: text/plain
Date: Fri, 31 May 2019 15:14:31 GMT
Content-Length: 0
{
  "errors": null,
  "restored": 2068,
  "skipped": 0
}
```

This will send the backup data in `backup.bin` to the server and restore lists. Lists are replaced on the server.

Note that the `ReadTimeout` in config must be set for a realistic value, otherwise no response is returned and restore may be interrupted.

```toml
# ReadTimeout is the maximum duration for reading the entire request, including the body.
ReadTimeout = "60m"

# WriteTimeout is the maximum duration before timing out writes of the response.
# It is reset whenever a new request's header is read.
WriteTimeout = "60m"
```

The backup will however keep running even if the connection is broken. Check status in the logs.

## Restoring from S3

AWS must be configured as described in "Backup to S3".

To specify s3 as a source, use the `src` parameter with the `s3://{bucket}/{path+file}` syntax described above.
 
Example:
```
curl -X POST "http://127.0.0.1:8080/xlist/restore?src=s3%3A%2F%2Fbackup-bucket%2Fpath%2Ffile-backup.bin"
```

This will restore from the `backup-bucket` bucket and the file at `path/file-backup.bin`.
 

## License

RankDB is licensed under [BSD 3-Clause Revised License](https://choosealicense.com/licenses/bsd-3-clause/). See LICENSE file.
   
## Contributing

You can contribute to the project by sending in a Pull Request.

Be sure to include tests for your pull requests. 
If you fix a bug, please add a regression test and make sure that new features have tests covering their functionality. 
