import redis
from redis.client import Pipeline

r = redis.Redis( host='redishost.edu', port=10011)
#r = redis.Redis( host='allindb.centralus.redisenterprise.cache.azure.net', port=10000, password='I4NCGKKUFmd6+VraDKrAOJrIF8TuN4bSsN+P2+2M96E=')

print(str(r.ping()))

x = r.execute_command('FT.SEARCH','idx:swc','"(@ok_platforms:{OSv2} @class:{green} @compatibleIDs:{39997})" NOCONTENT')
print(x)