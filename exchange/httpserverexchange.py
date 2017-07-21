import json
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

import simplejson

from exchange.AESCipher import AESCipher
from exchange.Logger import log

port = 9875

dataexchange = [
    {
        #翼 卡
        "consumer": {
            "ip":"",
            "url": "/bd/consumer?apikey=",
            "key": "f12793c2-7f0f-49d8-8151-0129596ae91b",#iv
            "secrete": "4mA1y7U3xKhXAwB3D4CRqmS6ie88XQmi"#privatekey
        },
        #生日管家
        "producer": {
            #"ip":"http://0.0.0.0:9874",
            "ip":"https://extapi.octinn.com",
            "url": "/nameService/phone?apikey=",
            "key": "cdcd8132-ae1a-4098-80f7-7abdf0313399",#iv
            "secrete": "mABKue3DGqxuNQh6Mj78nUQOOymzDSYF" #privatekey
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
        #get comsumer's data
        self.data_string = self.rfile.read(int(self.headers['Content-Length']))
        val = self.headers['Content-Length']
        self.send_response(200)
        self.end_headers()
        decrypt_data = 'null'
        con_data = 'null'
        if self.path.startswith('/bd/consumer'):

            if '?' in self.path:
                key = self.path.split('=')[1].strip()
                if not key==dataexchange[0]['consumer']['key']:
                    msg =self.errorMsg('apikey error')
                    self.wfile.write(bytes(json.dumps(msg),'utf8'))
                    return
                #解密consumer数据
                con_data = self.decryptData(self.data_string,dataexchange[0]['consumer'])
                if con_data ==None or len(con_data.strip())==0:
                    msg = self.errorMsg('can\'t decrypt data')
                    self.wfile.write(bytes(json.dumps(msg), 'utf8'))
                    return
                #加密发往producer的数据
                crypt_pro_data = self.encryptData(con_data,dataexchange[0]['producer'])
                #往producer发送加密数据并且接受响应结果(加密了的数据)
                #对数据进行拼接成data：'123213'
                jd = self.joinData(crypt_pro_data)
                resp_result = self.sendRequest(dataexchange[0]['producer'],jd)
                if not resp_result==None:
                    #数据解密
                    decrypt_data = self.decryptData(resp_result,dataexchange[0]['producer'])
                    # 数据加密并且发送
                    crypt_data = self.encryptData(decrypt_data, dataexchange[0]['consumer'])
                    crypt_jd = self.joinData(crypt_data)
                    self.wfile.write(crypt_jd)
                #解析数据and把每次交换的数据记录下来
                real_data = decrypt_data.encode('unicode_escape')

                log(str(con_data)+"\r\n"+str(real_data,'utf-8'))
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

        crypt = AESCipher(obj['secrete'][:16],obj['key'][:16])
        if(crypt == None):
            return
        data =simplejson.loads(com_data)['data']

        if(len(data)==0):
            return None

        real_com_data = crypt.decrypt(data)

        return str(real_com_data,'utf-8')


    def encryptData(self,pro_data,obj):
        '''
        encrypt data and send BrithdayManager
        :param yk_data:
        :return:type is bytes
        '''
        crypt = AESCipher(obj['secrete'][:16], obj['key'][:16])
        if (crypt == None):
            return

        crypt_pro_data = crypt.encrypt(pro_data)

        return crypt_pro_data

    def sendRequest(self,obj,data):
        '''
        send post request
        :param obj:about url msg
        :param data:
        :return:type is str
        '''
        url=obj['ip']+obj['url']+obj['key']
        str_data=None
        if (len(url) == 0):
            return

        headers={
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
        }
        req = urllib.request.Request(url,data,headers=headers)
        resp = urllib.request.urlopen(req)

        #接受响应回来的数据,type is bytes
        result = resp.read()
        #error
        if result==None:
            return None
        if len(result) !=0 :
            str_data = str(result,'utf-8').strip()
            real_data = json.loads(str_data)
            code = real_data.get('code', 'ok')
            #没有匹配到数据，返回None
            if code =='fail':
                return None

        return str_data

    def joinData(self,data):
        '''
        :param data: encrypt't data ;type is bytes
        :return: join's data;type is bytes
        '''
        json_data = {
            "data":str(data,'utf-8')
        }
        return bytes(json.dumps(json_data),"utf8")

    def errorMsg(self,msg):
        '''
        :param msg: error msg ;type is str
        :return: type is dict
        '''
        code = {}
        code['code'] = '{}'.format('faile')
        code['error'] = '{}'.format(msg)
        return code

def run():
    print('starting server...')

    # Server settings
    # Choose port 8080, for port 80, which is normally used for a http server, you need root access
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
    print('running server...{}'.format(port))
    httpd.serve_forever()


run()
