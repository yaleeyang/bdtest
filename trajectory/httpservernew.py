from http.server import BaseHTTPRequestHandler, HTTPServer
import os
import csv
import json
import redis
import urllib

data = []
pointer = 1
step = 4
test_file = './data/113854.csv'
port = 9876
#host = '106.15.57.39'
host = '127.0.0.1'
r = redis.StrictRedis(host=host)

geojson = {
    "geometry": {
        "type": "Point",
        "coordinates": []
    },
    "type": "Feature",
    "properties": {}
}

files = os.listdir('./sh_data')

rd_path = './rd_data/'
rd_files = os .listdir(rd_path)

deviceList = []
for file in files:
    deviceId = file.split('.')[0]
    deviceList.append(deviceId)

# HTTPRequestHandler class
class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):
    def getRedisData(self):
        global pointer
        geojson_v = []
        if (len(deviceList) != 0):
            deviceRequest = []
            for device in deviceList:
                deviceRequest.append(device + '_' + str(pointer))
                deviceRequest.append(device + '_' + str(pointer + step))

            results = r.mget(deviceRequest)
            for i in range(len(results) // 2):
                id = deviceRequest[i * 2].split('_')[0]
                if (results[i * 2] == None or results[i * 2 + 1] == None):
                    deviceList.remove(id)
                else:
                    rFrom, rTo = str(results[i * 2]).split(','), str(results[i * 2 + 1]).split(',')
                    json_v = {}
                    json_v['from'] = "{},{}".format(rFrom[4], rFrom[3])
                    json_v['to'] = "{},{}".format(rTo[4], rTo[3])
                    json_v['fromInfo'] = "start"
                    json_v['endInfo'] = "end"
                    geojson_v.append(json_v)

        message = json.dumps(geojson_v)
        self.wfile.write(bytes(message, "utf8"))
        pointer += step
        print(len(deviceList))

    def getRedisData1(self):
        global pointer
        geojson_v = []
        if (len(deviceList) != 0):
            deviceRequest = []
            for device in deviceList:
                deviceRequest.append(device + '_' + str(pointer))

            results = r.mget(deviceRequest)
            for i in range(len(results)):
                id = deviceRequest[i].split('_')[0]
                if (results[i] == None):
                    deviceList.remove(id)
                else:
                    rPt = str(results[i]).split(',')
                    json_v = {}
                    json_v['lat'] = "{}".format(rPt[3])
                    json_v['lng'] = "{}".format(rPt[4])
                    json_v['value'] = 1
                    #json_v['type'] = 1
                    geojson_v.append(json_v)

        message = json.dumps(geojson_v)
        self.wfile.write(bytes(message, "utf8"))
        pointer += step


    def getFilesData(self):
        geojson_v=[]
        request_arg='21'
        if(len(rd_files) != 0):
            deviceRequest = []
            for device in rd_files :
                deviceRequest.append(device)
            if '?' in self.path :#args exists
                request_args = str(urllib.parse.unquote(self.path.split('?', 1)[1])).split("&")
                dataId = request_args[len(request_args)-1]
                if('dataId' in dataId):#
                    request_arg=dataId.split("=")[1]
            for device in deviceRequest:
                if (len(request_arg)!=0 and request_arg in device ):
                    with open(rd_path+device,'r') as f:
                        for line in f:
                            fields = line.split(",")
                            if(fields[1].isdigit()):  #id exsist
                                json_v={}
                                json_v['lat'] = "{}".format(fields[3])
                                json_v['lng'] = "{}".format(fields[4])
                                json_v['value'] = 1
                                json_v['type'] ="{}".format(fields[len(fields)-1])
                                geojson_v.append(json_v)

        message = json.dumps(geojson_v)
        self.wfile.write(bytes(message,"utf8"))

    # GET
    def do_GET(self):
        # Send response status code
        self.send_response(200)

        # Send headers
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        if (self.path.startswith("/mapbox")):
            global pointer
            geojson['geometry']['coordinates'] = [data[pointer][3], data[pointer][4]]
            message = json.dumps(geojson)
            self.wfile.write(bytes(message, "utf8"))
            pointer += 1
        elif (self.path.startswith('/datav')):
            self.getRedisData()
        elif (self.path.startswith('/bubble')):
            self.getRedisData1()
        elif (self.path.startswith('/road')):
            self.getFilesData()


        # Send message back to client
        # message = "Hello world!"
        # Write content as utf-8 data


        return

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        BaseHTTPRequestHandler.end_headers(self)

def run():
    print('starting server...')

    global data
    with open(test_file, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)

    # Server settings
    # Choose port 8080, for port 80, which is normally used for a http server, you need root access
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
    print('running server...{}'.format(port))
    httpd.serve_forever()


run()
