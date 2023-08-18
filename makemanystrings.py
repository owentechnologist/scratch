import redis,sys

## This utility makes a bunch of String keys in Redis
## It expects you to pass 2 args: 
# 1) the prefix for the keys to be added
# example:  >python3 makemanystrings.py zew:animals:
# 2) the number of keys to be added
# if you like - you can also provide host and port and password and username as args
## example: >python3 makemanystrings.py zew:animals: 10000 redis-10150.homelab.local 10150
## example: >python3 makemanystrings.py zew:animals: 10000 redis-10150.homelab.local 10150 paszw0rd user@rd.com

# add the keys in Redis that match the provided prefix: 
def add_keys_to_redis(redis_proxy,target_prefix,number_of_keys):
    target_prefix = target_prefix
    batch_id_router = 0
    count = 0
    dbsize = redis_proxy.dbsize()
    print(f'redis has {dbsize} keys now [before writing new values]')
    while(count < number_of_keys):
        if ((count > 1) and (count % 1000)==0):
            print(f'So far, I have added {count} keys')
        pipeline = redis_proxy.pipeline()
        pipe_size = 5000 if (count < (number_of_keys-5000)) else (number_of_keys-count)
        print(f'pipe_size is now {pipe_size} batch_id_router = {batch_id_router}')
        innerc = 0
        while(innerc <= pipe_size):
            pipeline.set(target_prefix+str(count)+'{'+str(batch_id_router)+'}',target_prefix+str(count))
            innerc = innerc +1
            count = count +1
        pipeline.execute()
        batch_id_router = batch_id_router+1
    dbsize = redis_proxy.dbsize()
    print(f'redis has {dbsize} keys now [after writing new values]')
        
if __name__ == "__main__":
    # TODO: edit or supply as args the host and port to match your redis database endpoint:
    try:
        redis_proxy = redis.Redis(host='redis-PORT.dummy-FIXME.com', port=18031, password='C2iadH1hasCAk1zrHKj2nLwYrI0GvzdMn', decode_responses=True)
        redis_proxy.ping('ping')
    except:
        print(f'You may need to provide command line arguments for your host and port etc...')

    if len(sys.argv)>2:
        prefix = sys.argv[1]
        num_keys = int(sys.argv[2]) 
    if len(sys.argv)>3: ## add host and port
        redis_proxy = redis.Redis(host=sys.argv[3], port=int(sys.argv[4]), decode_responses=True) 
    if len(sys.argv)>5: ## add password (uses default username)
        redis_proxy = redis.Redis(host=sys.argv[3], port=int(sys.argv[4]), password=sys.argv[5], username='default', decode_responses=True) 
    if len(sys.argv)>6: ## add username
        redis_proxy = redis.Redis(host=sys.argv[3], port=int(sys.argv[4]), password=sys.argv[5], username=sys.argv[6], decode_responses=True) 
    if len(sys.argv)<2:
        print('Please supply a keyname prefix and the number of keys you wish to add like:\n zew:animals: 10000\n\nterminating ...')
        exit(0)

    add_keys_to_redis(redis_proxy,prefix,num_keys)
    