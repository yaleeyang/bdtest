from http.server import BaseHTTPRequestHandler, HTTPServer
import csv
import json

data = []
pointer = 1
step = 20
test_file = './sh_data/701396.csv'
port = 9877

geojson = {
    "geometry": {
        "type": "Point",
        "coordinates": []
    },
    "type": "Feature",
    "properties": {}
}

geojson_v = [
  {
    "from": "121.178260,31.153108",
    "to": "121.583413,31.172672",
    "fromInfo": "start",
    "toInfo": "end"
  }
]


# HTTPRequestHandler class
class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):
    # GET
    def do_GET(self):
        # Send response status code
        self.send_response(200)

        # Send headers
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        global pointer
        if (self.path.startswith("/mapbox")):
            geojson['geometry']['coordinates'] = [data[pointer][3], data[pointer][4]]
            message = json.dumps(geojson)
            self.wfile.write(bytes(message, "utf8"))
            pointer += 1
        elif (self.path.startswith("/datav")):
            geojson_v[0]['from'] = "{},{}".format(data[pointer][4], data[pointer][3])
            geojson_v[0]['to'] = "{},{}".format(data[pointer + step][4], data[pointer + step][3])
            message = json.dumps(geojson_v)
            self.wfile.write(bytes(message, "utf8"))
            if (pointer + step < len(data) - 1):
                pointer += step
            else:
                pointer = 1

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
