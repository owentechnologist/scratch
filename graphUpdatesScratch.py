import redis, time, sys
from redisgraph import Node, Edge, Graph, Path
from ast import literal_eval
from sys import argv

createGraph = False
verbose = False
batchSize = 1000
iterations = 5
shouldIndex = False

inarg = input("Should I create a new Graph? (y or n) ")
if(inarg == 'y'):
    createGraph = True
    print(f"createGraph = {createGraph}")
    inarg2 = input("Should I add an index for the nodes? (y or n) ")
    if(inarg2 == 'y'):
        shouldIndex = True

batchSize = int(input("how big a batch Size do you want? eg: 500  "))

iterations = int(input("how many times should we execute the batch? eg: 4  "))

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

if(createGraph): 
    myredis.delete('bulktest') # setup for clean test... < deletes this graph key if it exists

# Create graph key:
redis_graph = Graph('bulktest', myredis)

# Bulk updates example using unwind

#Establish starting points for nodes
baseDeviceID = 10000000
baseSignalFreq = 100
baseSignalStrength = 1
baseFailureCount = 0
channelBase = 7

if(createGraph):    
    for x in range(10000):
        params = {'dID':x+baseDeviceID,'cID':((x+channelBase)%3)}
        query = "MERGE (d:Device {id: $dID}) MERGE (c:Channel {id: $cID}) MERGE (d)-[rel:BELONGS_TO]->(c) RETURN 1 "
        result = redis_graph.query(query,params)
        x = (x+1)
        if(x % 5000 == 0):
            print(f"How many queries executed? {x}")

"""
format of each data_row is expected to look like this:
DeviceID,SignalFrequency,SignalStrength,FailureCount
[1287634683, 22, 4, 1]

"""
if(shouldIndex):
    indexCommand = "CREATE INDEX ON :Device(id)"
    redis_graph.query(indexCommand)

# prepare batch_query:
#batch_query = "MATCH (d:Device {id: $dID}) SET (d.) RETURN 1 "
batch_query = "MATCH (d:Device {id: row[0]}) SET d.SignalFrequency = row[1], d.SignalStrength = row[2], d.FailureCount = row[3] RETURN d"
querybase = " ".join(["UNWIND $rows AS", "row", batch_query])
print(f"\nquerybase looks like:\n\n{querybase}\n")
rows_wrap = []

ts1 = myredis.time() # timestamp1

#keep track of the updates we send to RedisGraph:
totalUpdates = 0
trange = iterations
rrange = batchSize
print("\n\nBeginning batch updates...\n\n")

for up in range(trange):
    for r in range(rrange):
        # program the row of data to be added to the batch and processed:
        data_row = f"['{baseDeviceID}','{baseSignalFreq}','{baseSignalStrength}','{baseFailureCount}']"
        if(verbose):
            print(f"data_row looks like this: {data_row}\n")
            print(f"data_row can be seen as a list which contains [{data_row.count('100')}] instances of the value: '100'")
        row = ",".join([quote_string(cell) for cell in literal_eval(data_row)])
        if(verbose):
            print(f"row looks like this: {row}\n")
        next_line = "".join(["[", row.strip(), "]"])
        if(verbose):
            print(f"next_line looks like this: {next_line}\n")
        rows_wrap.append(next_line)
        # modify our properties before repeating the loop:
        baseDeviceID = baseDeviceID+totalUpdates+1
        baseSignalFreq = 10*r
        baseSignalStrength = r%3
        baseFailureCount = r%4
        r = r+1

    up = up+1
    print(f"up counter is now: {up}")

    rows = "".join(["CYPHER rows=[", ",".join(rows_wrap), "]"])
    command = " ".join([rows, querybase])

    if(verbose):
        print(f"our query now looks like this: \n{command}")

    # Execute our command:
    redis_graph.query(command)

#End parent loop


totalUpdates = up * r
ts2 = myredis.time() # timestamp1
timestart = ts1[0]
timeend = ts2[0]
durationsecs = timeend-timestart
if(durationsecs<1):
    durationsecs = 1
rate = totalUpdates/durationsecs
print('Executed '+str(totalUpdates)+' updates in '+str(durationsecs)+' seconds')
print('Execution Rate == '+str(rate))

testquery = """GRAPH.QUERY 'bulktest' 'MATCH (x)--(y) WHERE x.id < 10005000  return x,y ORDER BY x.id DESC limit 150'"""
print(f"\n\nUSE REDIS-CLI or RedisInsight and issue the following query to see the modified nodes:  {testquery}")

