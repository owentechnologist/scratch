import redis, time, sys
from redisgraph import Node, Edge, Graph, Path
from ast import literal_eval

# stolen from: 
# https://github.com/RedisGraph/redisgraph-bulk-loader/blob/master/redisgraph_bulk_loader/bulk_update.py
def quote_string(cell):
    cell = cell.strip()
    try:
        float(cell) # Check for numeric
    except ValueError:
        if ((cell.lower() != 'false' and cell.lower() != 'true') and # Check for boolean
                (cell[0] != '[' and cell.lower != ']') and # Check for array
                (cell[0] != "\"" and cell[-1] != "\"") and # Check for double-quoted string
                (cell[0] != "\'" and cell[-1] != "\'")): # Check for single-quoted string
                cell = "".join(["\"", cell, "\""])
    return cell

try:
    myredis = redis.Redis( host='192.168.1.6', port=10007)
except redis.exceptions.ConnectionError as e:
        print("Could not connect to Redis server.")
        raise e

myredis.delete('bulktest') # setup for clean test... < deletes this graph key if it exists

# Create graph key:
redis_graph = Graph('bulktest', myredis)

# Bulk updates example using unwind

#Establish starting points for nodes
baseDeviceID = 64579400
baseSignalFreq = 100
baseSignalStrength = 1
baseFailureCount = 0
channelBase = 7

for x in range(100):
    params = {'dID':x+baseDeviceID,'cID':((x+channelBase)%3)}
    query = "MERGE (d:Device {id: $dID}) MERGE (c:Channel {id: $cID}) MERGE (d)-[rel:BELONGS_TO]->(c) RETURN 1 "
    result = redis_graph.query(query,params)
    x = (x+1)
    if(x % 50 == 0):
        print(f"How many queries executed? {x}")

"""
format of each data_row is expected to look like this:
DeviceID,SignalFrequency,SignalStrength,FailureCount
[1287634683, 22, 4, 1]

"""

# prepare batch_query:
#batch_query = "MATCH (d:Device {id: $dID}) SET (d.) RETURN 1 "
batch_query = "MATCH (d:Device {id: row[0]}) SET d.SignalFrequency = row[1], d.SignalStrength = row[2], d.FailureCount = row[3] RETURN d"
querybase = " ".join(["UNWIND $rows AS", "row", batch_query])
print(f"querybase looks like:\n\n{querybase}\n")
rows_wrap = []
for r in range(3):
    # program the row of data to be added to the batch and processed:
    data_row = f"['{baseDeviceID}','{baseSignalFreq}','{baseSignalStrength}','{baseFailureCount}']"
    print(f"data_row looks like this: {data_row}\n")
    print(f"data_row can be seen as a list which contains [{data_row.count('100')}] instances of the value: '100'")
    row = ",".join([quote_string(cell) for cell in literal_eval(data_row)])
    print(f"row looks like this: {row}\n")
    next_line = "".join(["[", row.strip(), "]"])
    print(f"next_line looks like this: {next_line}\n")
    rows_wrap.append(next_line)
    # modify our properties before repeating the loop:
    baseDeviceID = baseDeviceID+1
    baseSignalFreq = 10*r
    baseSignalStrength = r%3
    baseFailureCount = r%4

rows = "".join(["CYPHER rows=[", ",".join(rows_wrap), "]"])
command = " ".join([rows, querybase])

print(f"our query now looks like this: \n{command}")

# Execute our command:
redis_graph.query(command)
testquery = """GRAPH.QUERY "bulktest" "MATCH (x) WHERE x.id < 64579404 return x"""
print(f"\n\nUSE REDIS-CLI or RedisInsight and issue the following query to see the modified nodes:  {testquery}")

