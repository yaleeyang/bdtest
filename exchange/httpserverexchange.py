from http.server import BaseHTTPRequestHandler, HTTPServer
import os
import csv
import json
import simplejson
#from .AESCipher import AESCipher

port = 9875

dataexchange = [
    {
        "consumer": {
            "url": "/bd/consumer?apikey=",
            "key": "f12793c2-7f0f-49d8-8151-0129596ae91b",
            "secrete": "4mA1y7U3xKhXAwB3D4CRqmS6ie88XQmi"
        },
        "producer": {
            "url": "/bd/producer?apikey=",
            "key": "cdcd8132-ae1a-4098-80f7-7abdf0313399",
            "secrete": "mABKue3DGqxuNQh6Mj78nUQOOymzDSYF"
        }
    }
]

# HTTPRequestHandler class
class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    # GET
    def do_POST(self):
        self._set_headers()
        self.data_string = self.rfile.read(int(self.headers['Content-Length']))

        self.send_response(200)
        self.end_headers()

        data = simplejson.loads(self.data_string)
        print(data['test'])
        return

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        BaseHTTPRequestHandler.end_headers(self)

def run():
    print('starting server...')

    # Server settings
    # Choose port 8080, for port 80, which is normally used for a http server, you need root access
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
    print('running server...{}'.format(port))
    httpd.serve_forever()


run()
