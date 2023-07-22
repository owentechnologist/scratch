from rq import Queue
from redis import Redis
from RQtasks import count_words_at_url # added import!

redis_conn = Redis('redis-15076.c60.us-west-1-2.ec2.cloud.redislabs.com',15076,0,'b3a66bb6104cea5fb461f9e8863beee1f16a0ec2')
#redis_conn = Redis()
q = Queue(connection=redis_conn)
job = q.enqueue(count_words_at_url, 'http://nvie.com')
print(job)