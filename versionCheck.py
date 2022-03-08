import redis
#from distutils import StrictVersion

def get_version(connection):
#    “”"
#    Returns StrictVersion of Redis server version.
#   This function also correctly handles 4 digit redis server versions.
#    “”"
    try:
        version_string = connection.info('server')['redis_version']
    except ResponseError:  # fakeredis doesn’t implement Redis’ INFO command
        version_string = '5.0.9'
    return ('.'.join(version_string.split('.')[:3]))

connection = redis.Redis( host='192.168.1.59', port=10000)
x=get_version(connection)


print('VERSION: '+str(x))
