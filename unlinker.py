import redis,sys

## This utility unlinks keys in Redis (so that they later get removed)
## It expects you to pass the prefix for the keys you are targeting as an arg 
# example:  >python3 unlinker.py zew:animals:

# TODO: fix the host and port to match your redis database endpoint:
redis_proxy = redis.Redis(host='192.168.1.20', port=12000, decode_responses=True)

# remove the keys in Redis that cache the list of choices and ascii art: 
def unlink_keys_from_redis(target_prefix):
    target_prefix = target_prefix+'*'
    count = 0
    for i in redis_proxy.scan_iter(match=target_prefix,count=10000):
        redis_proxy.unlink(i)
        count = count +1
        if (count % 1000)==0:
            print(f'Unlinked 1000 keys like this one: {i}')

if __name__ == "__main__":
    if len(sys.argv)>1:
        prefix = sys.argv[1]
        unlink_keys_from_redis(prefix)
    else:
        print('Please supply a keyname prefix like: zew:animals:\n\nterminating ...')