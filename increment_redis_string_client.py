import redis,sys,time

## This client increments a string in an endless loop (until it hits an error):
# python3 increment_redis_string_client.py <string_keyname> 
## ex:
# python3 increment_redis_string_client.py target:1
# You *may* alter the default behavior by passing the following args:
# python3 increment_redis_string_client.py <string_keyname> <loop_size> <counter_value> <host> <port> <passwrd> <username>
## ex:
# python3 increment_redis_string_client.py incrk1 1000 1385
## If using this client to test AA failure scenarios you can:
# # run a simple lua script to crash the Redis instance of choice:
# EVAL "for index = 1,10000000 do redis.call('SCAN',0,'MATCH','*','COUNT','100000') end" 0
# When the AA instance stops responding you can use the command line tools to kill any shards it holds 
## <secret_util_command> stop_shards uids=1 force=yes
## When all is shutdown - reverse the process:
## <secret_util_command> start_shards uids=1
# while the above is happening - switch your client to the peer instance of the AA DB

if __name__ == "__main__":
    # TODO: pass in args or edit the host and port to match your redis database endpoint:
    try:
        redis_proxy = redis.Redis(host='197.168.1.20', port=10150, password='', decode_responses=True)
        redis_proxy.ping('ping')
    except:
        print(f'You may need to provide command line arguments for your host and port etc...')
    
    if len(sys.argv)>1:
        target_keyname = sys.argv[1]
        print(f'... using {target_keyname} as a lock.')
        loop_size = 1000
        counter = 0
        if len(sys.argv)>2: ## add how many incrementing loops to do (1000 default)
            loop_size = int(sys.argv[2]) 
        if len(sys.argv)>3: ## add counter value to expect in Redis
            counter = int(sys.argv[3])
        if len(sys.argv)>5: ## add host and port
            redis_proxy = redis.Redis(host=sys.argv[4], port=int(sys.argv[5]), decode_responses=True) 
        if len(sys.argv)>6: ## add password (uses default username)
            redis_proxy = redis.Redis(host=sys.argv[4], port=int(sys.argv[5]), password=sys.argv[6], username='default', decode_responses=True) 
        if len(sys.argv)>7: ## add username
            redis_proxy = redis.Redis(host=sys.argv[4], port=int(sys.argv[5]), password=sys.argv[6], username=sys.argv[7], decode_responses=True) 

        print(f'Starting instance of program - counter keyname is: {target_keyname} and the last expected value is: {counter}')
        for i in range(loop_size):
            time.sleep(.03)
            if(i%100==0):
                print(f'INCR Happening... LAST_KNOWN COUNTER: {counter}')

            try:
                counter_val = redis_proxy.incr(target_keyname)
                counter = counter_val
            except Exception as err:
                print(f'incr({target_keyname}) ... {err} \n LAST KNOWN COUNTER: {counter}')
                exit(1)    
        print(f'HEALTHY EXIT... LAST KNOWN COUNTER_VAL: {counter}')
    else:
        print('ALERT! You need to provide a keyname to use as a counter.')