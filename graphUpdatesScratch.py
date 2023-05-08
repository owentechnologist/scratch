import redis, time, sys
from redisgraph import Node, Edge, Graph, Path

# Edit the next 2 variable values to match your redis deployment:
redis_host = '192.168.1.21'
redis_port = 14787 
#redis_host = 'localhost'
#redis_port = 6379 


# edit this value to True if you want to see every string manipulation as query is constructed:
verbose = False

# the following default values will be adjusted by user through prompts during runtime:
createGraph = False
batchSize = 2000
iterations = 5
shouldIndex = False
num_device_nodes = 10000 # a fun initial size to test with

inarg = input("Should I create a new Graph? (y or n) ")
if(inarg == 'y'):
    createGraph = True
    print(f"createGraph = {createGraph}")
    inarga = input("how many device nodes should I create? eg: 20000  ")
    num_device_nodes = int(inarga)
    if(num_device_nodes>100000):
        print("Just so you know, this will take a bit of time...")
    if(num_device_nodes>500000):
        print(f"\n\n{num_device_nodes} is a huge number for a simple single threaded script like me... \nchanging to 500000\n\n")
        num_device_nodes=500000

inarg2 = input("Should I add an index for the nodes? (y or n) ")
if(inarg2 == 'y'):
    shouldIndex = True
    print(f"shouldIndex = {shouldIndex}")
    
print("\nWe are going to update many Device node properties ... this will be done in batches ")    
batchSize = int(input("--how many Device node updates do you want to do in a single batch? eg: 2000  "))
iterations = int(input("--how many times should we execute the update batch? eg: 5  "))
print(f"\nYou have elected to update the properties of {iterations*batchSize} nodes!\n")

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
    myredis = redis.Redis( host=redis_host, port=redis_port) 
except redis.exceptions.ConnectionError as e:
    print(f"Could not connect to Redis server at {redis_host}:{str(redis_port)}")
    raise e

if(createGraph): 
    myredis.delete('bulktest') # setup for clean test... < deletes this graph key if it exists

# Create connection to graph key:
redis_graph = Graph('bulktest', myredis)

# Bulk updates example using unwind

#Establish starting points for nodes
baseDeviceID = 10000000
baseSignalFreq = 100
baseSignalStrength = 1
baseFailureCount = 0

# this program generates queries like this:
"""
Each row is a comma-delimited set of values wrapped in []
Multiple rows are separated by commas
The total set of rows is then wrapped in []

CYPHER rows=[[64579400,100,1,0],[64579401,0,0,0],[64579402,10,1,1]] 
UNWIND $rows AS row MATCH (d:Device {id: row[0]}) 
SET d.SignalFrequency = row[1], d.SignalStrength = row[2], d.FailureCount = row[3] 
RETURN 1
"""

if(shouldIndex):
    indexCommand = "CREATE INDEX ON :Device(id)"
    redis_graph.query(indexCommand)
    indexCommand = "CREATE INDEX ON :Channel(id)"
    redis_graph.query(indexCommand)

# kept slow version for comparison but don't expect to use it once batch load works
want_slow=False

## create graph bulk version:
if(createGraph) and (want_slow==False):
    # break up inserts into batches of 1000:
    # ( we also use total_batches to determine number of channels for devices )
    total_batches = round((num_device_nodes/1000))
    rows_array = [] 
    printFlag = True
    for c in range(total_batches):
        print(f"\nRound {c} of batch inserts about to happen...\n\n")
        # establish query template for inserts:
        # good bquery = "UNWIND $rows AS row CREATE (d:Device  {id: row[0]}) WITH d,row MERGE (c:Channel {id: row[1]}) MERGE (d)-[rel:BELONGS_TO]->(c) RETURN 1"
        # experimental query follows:
        # UNWIND $rows AS row MERGE (p:updateTest{name: row[0]}) ON CREATE SET p.val=row[1] ON MATCH SET p.val=row[1]
        bquery = "UNWIND $rows AS row MERGE (d:Device  {id: row[0]}) ON CREATE SET d.channelid=row[1] ON MATCH SET d.dummyid=row[1] RETURN 1"
        # build rows to be later unwound:        
        insert_batchSize=int(num_device_nodes/total_batches)
        for i in range(insert_batchSize):
            data_r = f"[{(c*insert_batchSize)+i+baseDeviceID},{((i%total_batches)+1)}]"
            #data_line = "".join(["[", data_r.strip(), "]"])
            rows_array.append(data_r)
        # create and execute this batch query using the rows_array:
        cypher_rows = "".join(["CYPHER rows=[", ",".join(rows_array), "]"])
        q_command = " ".join([cypher_rows, bquery])
        # Execute our query/command:
        redis_graph.query(q_command)
        if(printFlag):
            print(f"First call to create nodes in our graph executed:\n\n {q_command}")
            printFlag = False
        rows_array = [] 

# kept slow version for comparison but don't expect to use it
# slow implementation of createGraph      
if(createGraph) and (want_slow):    
    for x in range(int(num_device_nodes)):
        params = {'dID':x+baseDeviceID,'cID':((x%3)+1)}
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


# prepare batch_query:
batch_query = "MATCH (d:Device {id: row[0]}) SET d.SignalFrequency = row[1], d.SignalStrength = row[2], d.FailureCount = row[3] RETURN 1"
querybase = " ".join(["UNWIND $rows AS", "row", batch_query])
print(f"\nquerybase looks like:\n{querybase}")
rows_wrap = []

ts1 = myredis.time() # timestamp1

#keep track of the updates we send to RedisGraph:
totalUpdates = 0
trange = iterations
rrange = batchSize
print("\n... Beginning batch updates...\n\n")

printFlag = False
device_id = baseDeviceID
for up in range(trange):    
    for r in range(rrange):
        # program the row of data to be added to the batch and processed:
        data_row = f"['{device_id}','{baseSignalFreq}','{baseSignalStrength}','{baseFailureCount}']"
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
        # basedeviceID should be equal to this batch number
        # times the size of the batch plus the instance of the batch
        # plus the baseDeviceID
        device_id = (up*rrange)+r+baseDeviceID
        baseSignalFreq = 10*r
        baseSignalStrength = r%3
        baseFailureCount = r%4

    print(f"up counter is now: {up}")

    rows = "".join(["CYPHER rows=[", ",".join(rows_wrap), "]"])
    command = " ".join([rows, querybase])

    if(printFlag):
        print(f"our query now looks like this: \n{command}")
        printFlag=False
    # Execute our command:
    redis_graph.query(command)
    rows_wrap = []

#End parent loop


totalUpdates = trange * (r+1)
ts2 = myredis.time() # timestamp1
timestart = ts1[0]
timeend = ts2[0]
durationsecs = timeend-timestart
if(durationsecs<1):
    durationsecs = 1
rate = totalUpdates/durationsecs
print('Executed '+str(totalUpdates)+' updates in '+str(durationsecs)+' seconds')
print('Execution Rate == '+str(rate))

testquery = f"GRAPH.QUERY 'bulktest' 'MATCH (x)--(y) WHERE x.id < {baseDeviceID+(iterations*batchSize)}  return x,y ORDER BY x.id DESC limit 300'"
print(f"\n\nUSE REDIS-CLI or RedisInsight and issue the following query to see the modified nodes:\n{testquery}")

