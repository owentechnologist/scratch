import redis
from redis.client import Pipeline

#myredis = redis.Redis( host='redishost.edu', port=6379)
myredis = redis.Redis( host='192.168.1.20', port=12000,decode_responses=True)
#myredis = redis.Redis( host='allindb.centralus.redisenterprise.cache.azure.net', port=10000, password='I4NCGKKUFmd6+VraDKrAOJrIF8TuN4bSsN+P2+2M96E=')

try:
    myredis.execute_command('FT.CREATE','idx_zew_revenue','PREFIX','1','zew:revenue:','SCHEMA','visitor_purchase_item_name','TEXT','visitor_purchase_item_cost','NUMERIC','SORTABLE')
except redis.exceptions.ResponseError as err:
    print(f'FT.CREATE ... {err} continuing on...')

# establish python-based stream workergroup with two members:
try:
    #myredis.xgroup_destroy('zew:{batch2}:revenue:stream','group1')
    myredis.xgroup_create('zew:{batch2}:revenue:stream','group1','0-0')
except:
    print('XGROUP_CREATE ... group already exists .. continuing on...')    
streamsdict = {'zew:{batch2}:revenue:stream': ">"}
for x in range(20):
    try:
        response = myredis.xreadgroup('group1','processorA',streams=streamsdict,count=1,noack=False)
        eventid = response[0][1][0][0]
        astring = response[0][1][0][1].get('visitor_purchase')
        #print(astring)
        itemcost = astring.split(":").pop(0)
        itemname = astring.split(":").pop(1)
        myredis.hset('zew:revenue:txid'+eventid,mapping={'visitor_purchase_item_name':itemname,'visitor_purchase_item_cost':itemcost})
        print(myredis.hgetall('zew:revenue:txid'+eventid))
    except:
        print('Any/All items in stream have been processed by this group')
        length = myredis.execute_command('XLEN',"zew:{batch2}:revenue:stream")
        print(f"There are {length} items in the stream")

sresult = myredis.execute_command('FT.AGGREGATE','idx_zew_revenue',"@visitor_purchase_item_cost:[35 80]","GROUPBY visitor_purchase_item_name")
''' 
 FT.AGGREGATE idx_zew_revenue "@visitor_purchase_item_cost:[35 80]" 
 GROUPBY 1 @visitor_purchase_item_name reduce SUM 1 @visitor_purchase_item_cost 
 AS total_earned APPLY "format(\"$%s\",@total_earned)" 
 AS total_dollars GROUPBY 2 @visitor_purchase_item_name @total_dollars
'''
print(sresult)

''' #below is some pipeline testing
myredis.set('mycounter{group1}',0) 
myredis.set('othercounter{group1}',10)

pipe = myredis.pipeline()
pipe.incrby('mycounter{group1}',1)
pipe.incrby('othercounter{group1}',-1)

list = pipe.execute()
for x in list:
    print('last modified value == ' +str(x))

# group keys together using a routing value placed between: {}

myredis.sadd('easternCities{groupA}','New York','Boston','Orlando')

myredis.sadd('easternStates{groupA}','NY','Mass','FL')

myredis.sadd('westernCities{groupB}','Los Angeles','Seattle','Vegas')

myredis.sadd('westernStates{groupB}','CA','WA','NV') 


 # this will work everytime:

myset = myredis.sunion('easternStates{groupA}','easternCities{groupA}')

print(myset)

result = myredis.sunionstore('statesCities{groupA}','easternStates{groupA}','easternCities{groupA}')

print(result)

 # this will probably not work:

#myset = myredis.sunion('easternStates{groupA}','westernCities{groupB}')
'''