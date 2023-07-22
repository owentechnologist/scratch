import redis,sys
from redis.commands.json.path import Path
import redis.commands.search
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.field import (
    GeoField,
    NumericField,
    TagField,
    TextField,
    VectorField,
)
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result

# this example was developed by looking at the test code available here:
# https://github.com/redis/redis-py/blob/master/tests/test_search.py

# TODO: fix the host and port and password to match your redis database endpoint:

r = redis.Redis(host='redis-12076.c60.us-west-1-2.ec2.cloud.redislabs.com', port=12076, password='XC8U71XnwGgSVEUXtGp25ZXI3x0NcDHT=', decode_responses=True)

# define the name for the Search Index:
idx = 'idx_srvc'

# populate the Redis DB with several JSON Objects (records of imaginary tests)
r.json().set("serviceX:MID892209121212901:1426349294842", "$", {"testID":"owtay:1","timecaptured":1426349294842,"machineid": "MID892209121212901", "results": [{"a":456,"b":787},{"a":32501,"b":787}, {"a":2981,"b":9663}]})
r.json().set("serviceX:MID892209121212901:1426359297654", "$", {"testID":"owtay:1","timecaptured":1426359297654,"machineid": "MID892209121212901", "results": [{"a":456,"b":787},{"a":32501,"b":787}, {"a":2981,"b":9663}]})
r.json().set("serviceX:MID892209121212902:1426449294842", "$", {"testID":"owtay:2","timecaptured":1426449294842,"machineid": "MID892209121212902", "results": [{"a":456,"b":787},{"a":32501,"b":787}, {"a":2981,"b":9663}]})
r.json().set("serviceX:MID892209121212902:1427359007654", "$", {"testID":"owtay:2","timecaptured":1427359007654,"machineid": "MID892209121212902", "results": [{"a":456,"b":787},{"a":32501,"b":787}, {"a":2981,"b":9663}]})

r.json().set("serviceX:MID892109221987141:1426549000819", "$", {"testID":"bobSmit:200","timecaptured":1426549000819,"machineid": "MID892109221987141", "results": [{"a":456,"b":787},{"a":32501,"b":787}, {"a":2981,"b":9663}]})
r.json().set("serviceX:MID892109221987141:1416359000614", "$", {"testID":"bobSmit:200","timecaptured":1416359000614,"machineid": "MID892109221987141", "results": [{"a":456,"b":787},{"a":32501,"b":787}, {"a":2981,"b":9663}]})

r.json().set("serviceX:MID541232418768321:1420019294888", "$", {"testID":"shayHass:32","timecaptured":1420019294888,"machineid": "MID541232418768321", "results": [{"a":456,"b":787},{"a":32501,"b":787}, {"a":2981,"b":9663}]})
r.json().set("serviceX:MID541232418768321:1428979007231", "$", {"testID":"shayHass:33","timecaptured":1428979007231,"machineid": "MID541232418768321", "results": [{"a":456,"b":787},{"a":32501,"b":787}, {"a":2981,"b":9663}]})

# Clean up any old Search Index Definition by dropping the index:
try:
    r.ft(idx).dropindex()
except redis.exceptions.ResponseError as exc:
    print(exc) 


definition = IndexDefinition(prefix=['service'],index_type=IndexType.JSON)
try:
    r.ft(idx).create_index((NumericField("$.timecaptured",sortable=True,as_name="timecap"),TagField("$.machineid",as_name="machineid")),definition=definition)
except redis.exceptions.ResponseError as rex:
    print(rex)

# Use the Search index to return document data based on varying facets like:
#  $.timecaptured (renamed as timecap)
#  $.machineid (renamed as machineid)
#Result{1 total, docs: 
# [Document {'id': 'serviceX:MID892209121212902:1426449294842', 
# 'payload': None, 
# 'json': '{"timecaptured":1426449294842,
# "machineid":"MID892209121212902",
# "results":[{"a":456,"b":787},{"a":32501,"b":787},{"a":2981,"b":9663}]}'}]}

print('\nThis next set of results showcases using the returned fields of the search API to fetch data that is not indexed\n'+
      'The filter used is a time-range using two timestamps to define the range\n')

searchResults = r.ft(idx).search(Query("@timecap:[1426300000000,1426459000000]").return_field("$.testID", as_field="testID").return_field("$.results[0].a", as_field="firstAResult"))
for x in searchResults.docs: 
    print('The testID for this result is:')
    print(x.testID)
    print('The firstA result for this matching test record is:')
    print(x.firstAResult)

print('\n\nThe following results are a complete dump of all data stored in each JSON object'+
      '\n the filter should match 4 JSON entries')
print(r.ft(idx).search("@machineid:{MID8922091212129*}"))

print('\n\nThis next result again showcases returning only a subset of the data via the search query API')
print(r.ft(idx).search(Query("@machineid:{MID892209121212901}").return_fields("machineid","timecap","$.testID")))

print('\n\nThis next example showcases Sorting data using search:\n')
req = aggregations.AggregateRequest("@timecap:[1426100000000,1426467000000]").sort_by("@machineid")
print(r.ft(idx).aggregate(req).rows)

print('\n\nThis next example showcases grouping and reducing data using search:\n')
req = aggregations.AggregateRequest("@timecap:[1426100000000,1426467000000]").group_by("@machineid",reducers.count().alias("how_many"))
print(r.ft(idx).aggregate(req).rows)