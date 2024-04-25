import redis

#NB: sping is an expensive command server side 
# the more general purpose = 'ping' is usually a better choice
connection = redis.Redis( host='bob.cache.azure.net', port=10000,password='VakhcJeA8U4PLjA1uThXexC8ql98Ov2BtVvVj76KBfQot=',decode_responses=True)
output_list = connection.execute_command('sping')
for x in output_list:
    print(x)
