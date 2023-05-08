import redis, time

r = redis.Redis( host='192.168.1.20', port=12000)
#r = redis.Redis( host='allindb.centralus.redisenterprise.cache.azure.net', port=10000, password='I4NCGKKUFmd6+VraDKrAOJrIF8TuN4bSsN+P2+2M96E=')

def runPerfTest1(r):#r is redis connection
    r.ping()
    startTime = time.time()
    #x = r.execute_command('FT.SEARCH','idx:swc','"(@ok_platforms:{OSv2} @class:{green} @compatibleIDs:{39997})" NOCONTENT')
    for x in range(200):
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((409900)+x)+' '+str((409900)+x)+'] @endRange:[3050 +inf] @startRange:[-inf 3050])')
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((509900)+x)+' '+str((509900)+x)+'] @endRange:[3050 +inf] @startRange:[-inf 3050])')
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((408900)+x)+' '+str((408900)+x)+'] @endRange:[1050 +inf] @startRange:[-inf 1050])')
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((508900)+x)+' '+str((508900)+x)+'] @endRange:[2050 +inf] @startRange:[-inf 2050])')
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((404900)+x)+' '+str((404900)+x)+'] @endRange:[2050 +inf] @startRange:[-inf 2050])')
    duration = time.time()-startTime
    #duration = duration * 1000000
    duration = duration/1000 #% 1.0
    print(y)
    print(f'Total time for 1000 search queries was: {duration*1000} seconds')
    print(f'Avg search query took: {duration} seconds')

def runPerfTest2(r):#r is redis connection
    r.ping()
    startTime = time.time()
    #x = r.execute_command('FT.SEARCH','idx:swc','"(@ok_platforms:{OSv2} @class:{green} @compatibleIDs:{39997})" NOCONTENT')
    for x in range(200):
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((409900)+x)+' '+str((409900)+x)+'] @endRange:[3050 +inf] @startRange:[-inf 3050])')
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((509900)+x)+' '+str((509900)+x)+'] @endRange:[3050 +inf] @startRange:[-inf 3050])')
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((408900)+x)+' '+str((408900)+x)+'] @endRange:[1050 +inf] @startRange:[-inf 1050])')
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((508900)+x)+' '+str((508900)+x)+'] @endRange:[2050 +inf] @startRange:[-inf 2050])')
        y = r.execute_command('FT.SEARCH','idxa_cardgroups','(@BIN_BASE:['+str((404900)+x)+' '+str((404900)+x)+'] @endRange:[2050 +inf] @startRange:[-inf 2050])')
    duration = time.time()-startTime
    #duration = duration * 1000000
    duration = duration/1000 #% 1.0
    print(y)
    print(f'Total time for 1000 search queries was: {duration*1000} seconds')
    print(f'Avg search query took: {duration} seconds')
