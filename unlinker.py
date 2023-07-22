import redis,sys,time

## This utility unlinks keys in Redis (so that they later get removed)
## It expects you to pass the prefix for the keys you are targeting as an arg 
# example:  >python3 unlinker.py zew:animals:

# TODO: fix the host and port to match your redis database endpoint:
redis_proxy = redis.Redis(host='redis-18031.mc290-0.us-central1-mz.gcp.cloud.rlrcp.com', port=18031, password='C2iadH1haCAk1zrHKj2nLwYrI0GvzdMn', decode_responses=True)

# remove the keys in Redis that match the provided prefix: 
def unlink_keys_from_redis(target_prefix):
    target_prefix = target_prefix+'*'
    count = 0
    for i in redis_proxy.scan_iter(match=target_prefix,count=10000):
        redis_proxy.unlink(i)
        count = count +1
        if (count % 1000)==0:
            print(f'So far, I have unlinked {count} keys like this one: {i}')

def pipeline_unlink_keys_by_prefix_from_redis(target_prefix):
    count = 0
    target_prefix = target_prefix+'*'
    pipe=redis_proxy.pipeline(transaction=False)
    for i in redis_proxy.scan_iter(match=target_prefix,count=100000):
        pipe.unlink(i)
        count = count +1
        if (count % 1000)==0:
            print(f'So far, I have identified {count} keys to unlink like this one: {i}')
    print(f'Unlinking {count} keys...')
    pipe.execute()    

def pipeline_unlink_all_keys_from_redis():
    count = 0
    pipe=redis_proxy.pipeline(transaction=False)
    for i in redis_proxy.scan_iter(match='*',count=100000):
        pipe.unlink(i)
        count = count +1
        if (count % 10000)==0:
            print(f'So far, I have identified {count} keys to unlink like this one: {i}')
    print(f'Unlinking {count} keys...')
    pipe.execute()    

if __name__ == "__main__":
    t1 = time.time()
    if len(sys.argv)>1:
        prefix = sys.argv[1]
        print(f'... Unlinking all keys starting with {prefix} in your target Redis instance.')
        pipeline_unlink_keys_by_prefix_from_redis(prefix)
    else:
        print('ALERT! Unlinking all keys in your target Redis instance.')
        print('To be more selective -- as an argument to this program: supply a keyname prefix like: zew:animals:')
        pipeline_unlink_all_keys_from_redis()
    time_millis = time.time()-t1
    print(f'time taken to execute unlinking was: {time_millis}')
