# API calls

## Root call
To see all available resources, go to: ```/api/?format=json```
## Resource calls
Resource consists of data and schema.

Resource data: ```/api/resource_name/?format=json```
Example: ```/api/tweet/?format=json```

Resource schema: ```/api/resource_name/schema/?format=json```
Example: ```/api/category/schema/?format=json```

## Representation
Resource's default mode of representation is a list of objects.
Every single object has a ```resource_uri``` attribute, which leads to a detailed representation of a particular object.

In the list mode ```meta``` container displays limit and offset (see parameters below), urls for previous and next portion of data (in case limit and offset are used) and total number of records in the output.
## Parameters
### Format
    /api/resource_name/?param1_name=param1_value&param2_name=param2_value&...&paramN_name=paramN_value
Parameters that serve as filters, allow for modifiers (```exact```, ```startswith```, ```lt```, ```gt```, ```ne```, etc.). Format: ```paramname__modifier=value```
Examples: ```...&description__startswith=RedLion```, ```...&description__contains=Controls```

All filters are case-insensitive.
See the full list of available filters and their modifiers in a /schema of each endpoint.

### Common parameters
* __format__ (at the moment 'json' is the only correct option)

* __username__ - not a param, but a part of authentication token (together with "api_key"). NB: this can also be sent as Authorization header.

* __api_key__ -  a part of authentication token (together with "username"). NB: this can also be sent as Authorization header.

* __limit__ - limits number of objects returned. Applicable only in case of detailed reports. Default: 36. Example: ...&limit=100

* __offset__ - number of records to skip from the beginning. Together with "limit" is used to divide data to pages (paginators). Default: 20. Example: ...&offset=40

## Resources
### Tweets
#### Plain list of tweets (GET)
    http://hostname/api/tweet/?username=username&api_key=api_key
#### Sorting tweets
By default news items are sorted by the field *created_by*, descending (latest first).
Custom sorting is performed using parameter order_by followed by the name of the field.

Examples.

Sorting by country:
    http://hostname/api/tweet/?order_by=country

Sorting by flood_probability descending:
    http://hostname/api/tweet/?order_by=-flood_probability

Sorting by multiple fields is done by adding an *order_by* parameter for each field to sort by:
    http://hostname/api/tweet/?order_by=-created_by&order_by=-flood_probability
WARNING! Order matters. Consider the following examples:
Sort by categories, and within a set of each category sort by sentiment descending: `order_by=categories&order_by=-sentiment` 
Sort by sentiment descending, and within sentiment value sort by title ascending: `order_by=-sentiment&order_by=title`
#### Filtering
Use names of fields for filtering in the same manners as parameters (see "Parameters" above):

    http://hostname/api/tweet/ \
        ?username=username \
        &api_key=4f23...d3c4 \
        &countries=United%20States

Filters can be combined:

    http://hostname/api/tweet/ \
        ?countries=United%20States \
        &categories=Generic \
        &sentiment__gte=2.5 \
        &created_at__lte=2018-01-12T12:30

Filtered data can be then sorted:

    http://hostname/api/tweet/ \
        ?countries=United%20States \
        &categories=Generic \
        &sentiment__gte=2.5 \
        &created_at__lte=2018-01-12T12:30 \
        &order_by=-created_at
#### Filtering by created_at
In addition to the standard modifiers (\_\_gt, \_\_lte, etc.) filtering by the field "created_at" can be performed using time-ranges. Time-range is a string that consists of two dates (start and end), divided by vertical bar (|):

    http://hostname/api/tweet/ \
        &created_at=2015-05-08T10:00|2015-05-09T12:15

It is possible to use both date- and time-stamps as values for ranges, and to combine them in the same query:

    http://hostname/api/tweet/ \
        &created_at=2015-05-08|2015-05-09T12:15

Time range can be specified in human readable format:

    http://hostname/api/tweet/ \
        &created_at=2 hours ago|now

It is possible to use other human readable keywords, e.g. "1 day ago", "January 12, 2017", "Saturday", etc.

Examples:

    http://hostname/api/tweet/ \
        &created_at=2012 Feb|yesterday

    http://hostname/api/tweet/ \
        &created_at=1st of Jul 2012|in 2 hours

**WARNING!** In those cases *two values* are necessary: start and end date (divided by vertical bar). The following example will cause **400 Bad Request**:

    http://hostname/api/tweet/ \
        &created_at=1 day ago

If the goal is to filter the records for the last day, use this:

    http://hostname/api/tweet/ \
        &created_at=1 day ago|now

Finally, there are reserved keywords, that don't require a pair of values: `today`, `yesterday`, `this week`, `last week`, `this month`, `last month`, `this year`, `last year`.

    http://hostname/api/tweet/ \
        ?country=Canada \
        &created_at=yesterday

Filters based on time-ranges and reserved keywords are *inclusive*, i.e. they automatically stretch filters from the beginning of the starting date (0:00, or 12am) to the end of the ending date (23:59:59 or 11:59pm). So, on the 1st of Jan, 2019 the example above would be equivalent to the following:

    http://hostname/api/tweet/ \
        ?country=Canada \
        &created_at=2018-12-31T00:00:00|2018-12-31T23:59:59

#### Search
Search is a special case of filtering. Use parameter `&search=` for searching:
http://hostname/api/tweet/?search=policy

Search are performed by the text (or list of items) stored the following fields (properties): `text`, `tokens`, `place`, `user_name`, `user_location`, `user_description`

#### Search with filtering
Search can be combined with filters:

    http://hostname/api/tweet/?countries=United%20States
    	&flood_propbability__gte=0.6
    	&created_at__lte=2018-01-12T12:30
    	&search=don't trust cruise ships
    	&order_by=country
    	&order_by=-created_at

#### Aggregated data (GET)
Tweets can be aggregated by geo-location (path in the response content: `["features"][docindex]["geometry"]["coordinates"]`) and timestamp (path: `["features"][docindex]["properties"]["created_at"]`).

If any of aggregation parameter appear in the request, the response contains additional field "aggregations", where summarised number of documents are being gathered in "buckets", and sorted accordingly (see below).

##### Aggregation by geo-location

    http://hostname/api/tweet/?countries=United%20States
    	&agg_hotspot=true
    	&agg_precision=4
    	&agg_size=1000

`agg_precision` parameter (integer) takes values between 1 and 12 and indicates how precise an aggregation is on a map: *1* is 5,009.4km x 4,992.6km, *12* is 3.7cm x 1.9cm (full list - https://www.elastic.co/guide/en/elasticsearch/reference/6.2//search-aggregations-bucket-geohashgrid-aggregation.html#_cell_dimensions_at_the_equator). Default is 5. Be careful to use very high precision, as it is RAM-greedy.

`agg_size` - maximum number of buckets to return (defaults to 10,000). When results are trimmed, buckets are prioritised based on the volumes of documents they contain (`doc_count`).

Buckets are sorted by `doc_count` descending (bigger at the top).

##### Aggregation by created_at
    http://hostname/api/tweet/?countries=United%20States
    	&agg_timestamp=true
    	&agg_precision=90m

`agg_precision` parameter defines a  interval for collecting tweets. Available expressions for interval: `year` (`1y`), `quarter` (`1q`), `month` (`1M`), `week` (`1w`), `day` (`1d`), `hour`(`1h`), `minute` (`1m`), `second` (`1s`). Fractional time values are not supported, but it is possible to achieve the goal shifting to another time unit (e.g., `1.5h` could instead be specified as `90m`). **Warning**: time intervals larger than than days do not support arbitrary values but can only be one unit large (e.g. `1y` is valid, `2y` is not).

Buckets are sorted by timestamps of the intervals, ascending.
