import redis
import os

path = './sh_data/'
host = 'localhost'
expiretime = 157680000 #5 years in seconds

r = redis.StrictRedis(host=host)

files = os.listdir(path)
for file in files:
    index = 1
    with open(path + file) as f:
        print("set file:" + file)
        pipeline = r.pipeline()
        for line in f:
            parts = line.split(',')
            if (parts[1].isdigit()):
                key = "{}_{}".format(parts[1], index)
                pipeline.set(key, line)
                pipeline.expire(key, expiretime)
                index += 1
        pipeline.execute()