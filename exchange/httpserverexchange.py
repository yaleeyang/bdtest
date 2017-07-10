import json
import urllib
from http.server import BaseHTTPRequestHandler, HTTPServer

import simplejson

from exchange.AESCipher import AESCipher


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
SURL='http://0.0.0.0:9874'

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
        val = self.headers['Content-Length']
        self.send_response(200)
        self.end_headers()

        if self.path.startswith('/bd/consumer'):
            if '?' in self.path:
                key = self.path.split('=')[1].strip()
                result = self.decryptConsumerData(YK_data=self.data_string,Key=key)
                self.wfile.write(bytes(result,'utf8'))
        return


    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        BaseHTTPRequestHandler.end_headers(self)


    def decryptConsumerData(self,YK_data,Key):
        '''
        decrypt ConsumerData and handle
        :param YK_data:
        :param Key:
        :return:
        '''

        if len(Key) == 0 or len(YK_data)==0:
            return

        crypt = AESCipher(Key[:16],dataexchange[0]['consumer']['secrete'][:16])
        if(crypt == None):
            return
        real_yk_data = crypt.decrypt(YK_data)

        yk_data =simplejson.loads(real_yk_data)

        print('ge平台解析后的数据：'+str(yk_data))

        return self.encryptConsumerData(yk_data)


    def encryptConsumerData(self,yk_data):
        '''
        encrypt data and send BrithdayManager
        :param yk_data:
        :return:
        '''
        crypt = AESCipher(dataexchange[0]['producer']['key'][:16], dataexchange[0]['producer']['secrete'][:16])

        json_yk_data=json.dumps(yk_data)

        crypt_yk_data = crypt.encrypt(json_yk_data)

        result = self.sendRequest(SURL+dataexchange[0]['producer']['url']+dataexchange[0]['producer']['key'],crypt_yk_data)

        return result

    def sendRequest(self,url, data):
        '''
        send post request
        :return:
        '''
        if (len(url) == 0):
            return

        req = urllib.request.Request(url, data=data)
        resp = urllib.request.urlopen(req)
        print('send successful')

        #接受响应回来的数据
        result = resp.read()
        if len(result) !=0 :
            str_data = str(result,'utf-8')
            #对响应回来的数据进行截取
            index = str_data.index('[')
            data = str_data[index:]
           #json_data = json.dumps(data)
        return data


def run():
    print('starting server...')

    # Server settings
    # Choose port 8080, for port 80, which is normally used for a http server, you need root access
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
    print('running server...{}'.format(port))
    httpd.serve_forever()


run()
