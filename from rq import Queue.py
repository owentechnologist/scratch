from rq import Queue
from redis import Redis
import requests

def count_words_at_url(url):
    resp = requests.get(url)
    return len(resp.text.split())

q = Queue(connection=Redis())

result = q.enqueue(count_words_at_url, 'http://nvie.com')

print(result)
