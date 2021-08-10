import redis
from redis.client import Pipeline

#myredis = redis.Redis( host='redishost.edu', port=6379)
myredis = redis.Redis( host='allindb.centralus.redisenterprise.cache.azure.net', port=10000, password='I4NCGKKUFmd6+VraDKrAOJrIF8TuN4bSsN+P2+2M96E=')

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
