import redis, time
import socket
socket.setdefaulttimeout(10)
sock = socket.socket()
sock.timeout
10.0

r = redis.Redis( host='192.168.1.65', port=10020, password='')
r.ping()
howmany = 10001
x = 10
timetotal1 = 0
timetotal2 = 0
timetotal3 = 0
x = 1

pipe = r.pipeline(transaction=False)
ts1 = time.time() # timestamp1
while x<howmany:
  pipe.hgetall('rps:comment:{groupA}'+str(x%20))
  pipe.hgetall('rps:comment:{groupB}'+str(x%20))
  x = x+1
list = pipe.execute()
ts2 = time.time() # timestamp2
timetotal1=ts2-ts1
#for l in list:
 #   print('dictionary == ' +str(l))

x = 1

pipe = r.pipeline(transaction=True)
ts1 = time.time() # timestamp1
while x<howmany:
  pipe.hgetall('rps:comment:{groupA}:'+str(x%20))
  x = x+1
list = pipe.execute()
ts2 = time.time() # timestamp2
timetotal2=ts2-ts1

x = 1

pipe = r.pipeline(transaction=True)
ts1 = time.time() # timestamp1
while x<howmany:
  pipe.hgetall('rps:comment:{groupB}:'+str(x%20))
  x = x+1
list = pipe.execute()
ts2 = time.time() # timestamp2
timetotal3=ts2-ts1

print('time for Transaction False == '+str(timetotal1))
print('time for Transaction True 1 thread == '+str((timetotal2)))
print('time for Transaction True 2nd thread == '+str((timetotal3)))
print('time for Transaction True if in series == '+str((timetotal2+timetotal3)))
