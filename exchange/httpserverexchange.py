import json
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

import simplejson

from exchange.AESCipher import AESCipher
from exchange.Logger import log

port = 9875

dataexchange = [
    {
        "consumer": {
            "ip":"",
            "url": "/bd/consumer?apikey=",
            "key": "f12793c2-7f0f-49d8-8151-0129596ae91b",
            "secrete": "4mA1y7U3xKhXAwB3D4CRqmS6ie88XQmi"
        },
        "producer": {
            "ip":"http://0.0.0.0:9874",
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
        val = self.headers['Content-Length']
        self.send_response(200)
        self.end_headers()

        if self.path.startswith('/bd/consumer'):
            code={}
            if '?' in self.path:
                key = self.path.split('=')[1].strip()
                if not key==dataexchange[0]['consumer']['key']:
                    code['code']='{}'.format('faile')
                    code['error'] = '{}'.format('apikey error')
                    self.wfile.write(bytes(json.dumps(code),'utf8'))
                    return
                #解密consumer数据
                con_data = self.decryptData(self.data_string,dataexchange[0]['consumer'])
                #加密发往producer的数据
                crypt_pro_data = self.encryptData(con_data,dataexchange[0]['producer'])
                #往producer发送加密数据并且接受响应结果(加密了的数据)
                resp_result = self.sendRequest(dataexchange[0]['producer'],crypt_pro_data)
                #数据解密
                decrypt_data = self.decryptData(resp_result,dataexchange[0]['producer'])
                #把每次交换的数据记录下来
                log(str(con_data)+"\r\n"+str(decrypt_data))

                #数据加密并且发送
                crypt_data = self.encryptData(decrypt_data,dataexchange[0]['consumer'])
                self.wfile.write(crypt_data)
        return


    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        BaseHTTPRequestHandler.end_headers(self)

    def decryptData(self,com_data,obj):
        '''
        decrypt ConsumerData and handle
        :param YK_data:
        :param Key:
        :return:type is list
        '''
        if len(obj['key']) == 0 or len(com_data)==0:
            return

        crypt = AESCipher(obj['key'][:16],obj['secrete'][:16])
        if(crypt == None):
            return
        real_com_data = crypt.decrypt(com_data)

        data =simplejson.loads(real_com_data)

        return data


    def encryptData(self,pro_data,obj):
        '''
        encrypt data and send BrithdayManager
        :param yk_data:
        :return:type is bytes
        '''
        crypt = AESCipher(obj['key'][:16], obj['secrete'][:16])
        if (crypt == None):
            return
        json_pro_data=json.dumps(pro_data)

        crypt_pro_data = crypt.encrypt(json_pro_data)

        return crypt_pro_data

    def sendRequest(self,obj,data):
        '''
        send post request
        :param obj:
        :param data:
        :return:type is str
        '''
        url=obj['ip']+obj['url']+obj['key']

        if (len(url) == 0):
            return

        req = urllib.request.Request(url,data)
        resp = urllib.request.urlopen(req)

        #接受响应回来的数据
        result = resp.read()
        if len(result) !=0 :
            str_data = str(result,'utf-8')
            #对响应回来的数据进行截取
            if '*' in str_data:
                index = str_data.index('*')+1
                resp_data = str_data[index:].strip('\r\n')
                #json_data = json.dumps(data)
        return resp_data


def run():
    print('starting server...')

    # Server settings
    # Choose port 8080, for port 80, which is normally used for a http server, you need root access
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
    print('running server...{}'.format(port))
    httpd.serve_forever()


run()
