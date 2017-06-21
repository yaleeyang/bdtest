import urllib.request
import json
import csv
import os

from random import randint
from time import sleep

START_DATE = '2016-01-01 00:00:00'
END_DATE = '2017-05-01 00:00:00'
PAGE_SIZE = 1000

def save_csv (tid, dataArr):
    filename = './data/' + str(tid) + '.csv'

    if os.path.exists(filename):
        append_write = 'a'
    else:
        append_write = 'w'

    with open(filename, append_write, newline='') as csvfile:
        writer = csv.writer(csvfile)

        if append_write == 'w':
            writer.writerow(["datetime", "terminalId", "time", "lat", "lon", "direction", "speed"])

        for part in dataArr:
            row = []
            for _, value in part.items():
                row.append(value)
            writer.writerow(row)

def postTerminalId(tid):
    url = 'http://121.43.168.120:8090/API/GIS.ashx?action=Get'
    count = 1
    while True:
        payload = {
            "terminalid": tid,
            "datetimestart": START_DATE,
            "datetimeend": END_DATE,
            "pageindex": count,
            "pagesize": PAGE_SIZE
        }
        req = urllib.request.Request(url)
        req.add_header('Content-Type', 'application/json')

        sleepms = randint(1, 9) / 10
        print("request {} page:{} size:{} sleep:{}s".format(tid, count, PAGE_SIZE, sleepms))
        sleep(sleepms)
        response = urllib.request.urlopen(req, json.dumps(payload).encode('utf-8')).read()
        resJson = json.loads(response.decode())
        if resJson['IsSuccess']:
            save_csv(tid, resJson['Value'])
            count += 1
        else:
            break

# postTerminalId(704234)

f = open('./deviceid.bin', 'rb')
data = json.load(f)['Value']
f.close()

for tdata in data:
    tid = tdata['terminalId']
    postTerminalId(tid)
