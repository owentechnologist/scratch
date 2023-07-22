import redis,sys

## This utility makes a bunch of String keys in Redis
## It expects you to pass 2 args: 
# 1) the prefix for the keys to be added
# example:  >python3 makemanystrings.py zew:animals:
# 2) the number of keys to be added

# TODO: fix the host and port to match your redis database endpoint:
redis_proxy = redis.Redis(host='redis-18031.mc290-0.us-central1-mz.gcp.cloud.rlrcp.com', port=18031, password='C2iadH1haCAk1zrHKj2nLwYrI0GvzdMn', decode_responses=True)

# add the keys in Redis that match the provided prefix: 
def add_keys_to_redis(target_prefix,number_of_keys):
    target_prefix = target_prefix
    batch_id_router = 0
    count = 0
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
        
if __name__ == "__main__":
    if len(sys.argv)>2:
        prefix = sys.argv[1]
        num_keys = int(sys.argv[2]) 
        add_keys_to_redis(prefix,num_keys)
    else:
        print('Please supply a keyname prefix and the number of keys you wish to add like:\n zew:animals: 10000\n\nterminating ...')