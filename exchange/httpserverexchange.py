import json
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
import simplejson

from exchange.AESCipher import AESCipher
from exchange.Utils import log
from exchange.Utils import ReadConfig


port = 9877

#load config
rc = ReadConfig('./ge.conf')
consumer = rc.getItems('consumer')
producer = rc.getItems('producer')
dataexchange = [
    {
        #翼 卡
        "consumer": {
            "ip":consumer['ip'],
            "url": consumer['url'],
            "key": consumer['key'],#iv
            "secrete": consumer['secrete']#privatekey
        },
        #生日管家
        "producer": {
            "ip":producer['ip'],
            "url": producer['url'],
            "key": producer['key'],#iv
            "secrete": producer['secrete'] #privatekey
        }
    }
]


# HTTPRequestHandler class
class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        BaseHTTPRequestHandler.end_headers(self)

    # GET
    def do_POST(self):

        self._set_headers()
        #get comsumer's data
        self.data_string = self.rfile.read(int(self.headers['Content-Length']))
        val = self.headers['Content-Length']
        self.send_response(200)
        #no match data logging  null
        real_data = 'null'
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
                    real_data = bytes(decrypt_data,'utf-8').decode('unicode_escape')
                else :
                    msg = self.errorMsg('not match infomation！',phone=con_data)
                    self.wfile.write(bytes(json.dumps(msg), 'utf8'))
                log(str(con_data)+"\r\n"+real_data)
        else:
            self.wfile.write(bytes(json.dumps(self.errorMsg('vistis url error！')), 'utf8'))
        return


    # def end_headers(self):
    #     self.send_header('Access-Control-Allow-Origin', '*')
    #     BaseHTTPRequestHandler.end_headers(self)

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

        try:
            data =simplejson.loads(com_data).get('data',None)

            if data==None or (len(data)==0):
                return None

            real_com_data = crypt.decrypt(data)

            return str(real_com_data, 'utf-8')

        except BaseException:
            return None

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
            str_data = str(result,'utf-8')
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

    def errorMsg(self,msg,phone=None):
        '''
        :param msg: error msg ;type is str
        :return: type is dict
        '''
        code = {}
        code['code'] = '{}'.format('fail')
        code['error'] = '{}'.format(msg)
        if not phone == None:
            #把｛'phone'：'12312312'｝放入到code中
            p = json.loads(phone)
            code.update(p)
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
