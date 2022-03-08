from rq import Queue
from redis import Redis
from RQtasks import count_words_at_url # added import!

redis_conn = Redis('redis-15039.c15911.us-central1-mz.gcp.cloud.rlrcp.com',15039,0,'XtOAiSh5ClgbszHf9Z7iOfEX4vVypevW')
#redis_conn = Redis()
q = Queue(connection=redis_conn)
job = q.enqueue(count_words_at_url, 'http://nvie.com')
print(job)