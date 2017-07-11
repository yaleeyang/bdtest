import redis
import os

path = './sh_data/'
host = 'localhost'
expiretime = 157680000 #5 years in seconds

r = redis.StrictRedis(host=host)

files = os.listdir(path)
fileslen = len(files)
inprocess = 0
for file in files:
    inprocess += 1
    index = 1
    with open(path + file) as f:
        print("set file:" + file + " " + str(inprocess) + "/" + str(fileslen))
        pipeline = r.pipeline()
        for line in f:
            parts = line.split(',')
            if (parts[1].isdigit()):
                key = "{}_{}".format(parts[1], index)
                pipeline.set(key, line)
                pipeline.expire(key, expiretime)
                index += 1
        pipeline.execute()