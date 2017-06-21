import redis
import os

path = './sh_data/'
host = 'localhost'
r = redis.StrictRedis(host=host)

files = os.listdir(path)
for file in files:
    index = 1
    with open(path + file) as f:
        for line in f:
            parts = line.split(',')
            if (parts[1].isdigit()):
                key = "{}_{}".format(parts[1], index)
                r.set(key, line)
                r.persist(key)
                print("set:" + key)
                index += 1
