import redis, time
from redisgraph import Node, Edge, Graph, Path

myredis = redis.Redis( host='192.168.1.6', port=10007)
#myredis = redis.Redis( host='192.168.1.59', port=6379)

#myredis.flushdb # setup for clean test... <-- WIPES ALL DATA!!

myredis.delete('problem') # setup for clean test... < deletes this graph key if it exists

redis_graph = Graph('problem', myredis)
version = redis_graph.version
print('VERSION: '+str(version))

baseproblem = 64579400
basecomponent = 1083500
ts1 = myredis.time() # timestamp1
opstarget = 5000
opscount = 0

x=0
while x < opstarget:
    params = {'cid0':x+basecomponent,'pid0':x+baseproblem}
    query = "MERGE (c:Component {id: $cid0}) MERGE (p:Problem {id: $pid0}) MERGE (p)-[rel:BELONGS_TO]->(c) RETURN 1 "
    result = redis_graph.query(query,params)
    x = (x+1)
    if(x % 1000 == 0):
        print('What is X: '+str(x))
    
opscount = opscount + x

print('\nmoving on to the deletions and merges...')
x=0
while x < opstarget:
    params = {'cid0':x+basecomponent,'pid0':x+baseproblem}
    if (x % 2 == 0):
        result = redis_graph.query(
            "MATCH (p:Problem {id: "+str(baseproblem+x)+"})-[relC]->(c:Component) OPTIONAL MATCH (p)-[relM]->(m:Milestone) DELETE relM,relC RETURN 1 "
        )

    if (x % 2 == 1):
        query = "MERGE (c:Component {id: $cid0}) MERGE (p:Problem {id: $pid0}) MERGE (p)-[rel:BELONGS_TO]->(c) RETURN 1 " 
        result = redis_graph.query(query,params)
        #result.pretty_print()

    x=x+1
    if(x % 1000 == 0):
        print('What is X: '+str(x))

opscount = opscount + x

print('\nmoving on to the just MATCH calls...')
x=0
while x < (opstarget*10):
    params = {'cid0':(x%opstarget)+basecomponent,'pid0':(x%opstarget)+baseproblem}
    query = "MATCH (c:Component {id: $cid0}) MATCH (p:Problem {id: $pid0}) MATCH (p)-[rel:BELONGS_TO]->(c) RETURN 1 " 
    result = redis_graph.query(query,params)
    #result.pretty_print()

    x=x+1
    if(x % 1000 == 0):
        print('What is X: '+str(x))

opscount = opscount + x

ts2 = myredis.time() # timestamp1
timestart = ts1[0]
timeend = ts2[0]
durationsecs = timeend-timestart
rate = opscount/durationsecs
print('Executed '+str(opscount)+' ops in '+str(durationsecs)+' seconds')
print('Execution Rate == '+str(rate))
