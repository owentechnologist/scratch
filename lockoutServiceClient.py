import redis,sys,time,random

## This client uses two LUA Scripts to manage locking out other instances
# You must pass the following command line argument:
# python3 lockoutServiceClient.py <lock_keyname> 
## ex:
# python3 lockoutServiceClient.py lock:2
# You *may* alter the default behavior by passing the following args:
# python3 lockoutServiceClient.py <lock_keyname> <loop_size> <host> <port> <passwrd>
## ex:
# python3 lockoutServiceClient.py lock:2 10 192.168.1.20 10150 ''

# get the id in Redis for the interesting lock: (or get zero)
def ask_unlock_value(target_keyname,redis_proxy):
    lua_unlock = "local response = 0 local ts=redis.call('TIME')[2]..(math.random(100,200)) if redis.call('EXISTS',KEYS[1]) ==1 then return response else redis.call('SET',KEYS[1],ts) end return ts"
    i = redis_proxy.execute_command('EVAL',lua_unlock,1,target_keyname)
    print(f'value returned from ask_unlock = {i}')
    return i

# extend the lease on the lock as long as this service is alive
def extend_lock(target_keyname,lock_id,redis_proxy):
    lua_extend = "local unlock = ARGV[1] local lockval = redis.call('GET',KEYS[1]) if lockval == unlock then redis.call('SET',KEYS[1],lockval,'PX',ARGV[2]+(math.random(100,200))) return lockval else return 0 end"
    i = redis_proxy.execute_command('EVAL',lua_extend,1,target_keyname,lock_id,500)
    print(f'extending lock returned {i}')
    return i

if __name__ == "__main__":
    # TODO: fix the host and port to match your redis database endpoint:
    redis_proxy = redis.Redis(host='192.168.1.20', port=10150, password='', decode_responses=True)
    if len(sys.argv)>1:
        target_keyname = sys.argv[1]
        print(f'... using {target_keyname} as a lock.')
        loop_size = 10
        if len(sys.argv)>2:
            loop_size = int(sys.argv[2]) 
        if len(sys.argv)>4:
            redis_proxy = redis.Redis(host=sys.argv[3], port=int(sys.argv[4]), password='', decode_responses=True) 
        if len(sys.argv)>5:
            redis_proxy = redis.Redis(host=sys.argv[3], port=int(sys.argv[4]), password=sys.argv[5], decode_responses=True) 
        lock_id = 0
        for i in range(loop_size):
            #bake in a likely failure to test swapping ownership of the lock:
            if(random.randint(1,3)>1):
                time.sleep(.3)
                print(f'Sleeping for 300 millis')

            if int(lock_id)>0:
                extend_lock(target_keyname,lock_id,redis_proxy)
            else:
                lock_id = ask_unlock_value(target_keyname,redis_proxy)
        #it is important to reset the local lock_id between tries:
        lock_id = 0
        for i in range(loop_size):
            #bake in a likely failure to test swapping ownership of the lock:
            if(random.randint(1,3)>1):
                time.sleep(.3)
                print(f'Sleeping for 300 millis')

            if int(lock_id)>0:
                extend_lock(target_keyname,lock_id,redis_proxy)
            else:
                lock_id = ask_unlock_value(target_keyname,redis_proxy)
    else:
        print('ALERT! You need to provide a keyname to use as a lock.')