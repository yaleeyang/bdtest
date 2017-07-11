import json
import random

import simplejson
from exchange.AESCipher import AESCipher


BKey="cdcd8132-ae1a-4098-80f7-7abdf0313399"
BIV="mABKue3DGqxuNQh6Mj78nUQOOymzDSYF"


class BirthdayResiveService(object):

    def decryptProducerData(self,YK_data,Key):
        if len(Key) == 0 or len(YK_data)==0:
            return
        crypt = AESCipher(Key[:16],BIV[:16])
        if(crypt == None):
            return

        #encrypt  GE transmit data
        real_yk_data = crypt.decrypt(YK_data)
        yk_data =simplejson.loads(real_yk_data)#list
        print('bm接受ge发送过来的解密数据：'+str(yk_data))
        result = BirthdayResiveService.handl_data(self,yk_data)
        print('bm处理过后的数据:'+result)
        return result

    def encryptData(self,data):
        if len(data)==0:
            return
        crypt = AESCipher(BKey[:16],BIV[:16])
        if (crypt == None):
            return
        crypt_data = crypt.encrypt(data)

        return crypt_data


    def handl_data(self,datas):
        jsongeo_v=[]
        # deal with data
        for data in datas:
            #imitate handle result
            json_v = {}
            num = random.randint(0,10)
            if num%2 == 0:
                json_v['code']='{}'.format('ok')
                json_v['phone']='{}'.format(data['phone'])
                json_v['age']='{}'.format('26')
                json_v['gender']='{}'.format('女')
            else:
                json_v['code']='{}'.format('fail')
                json_v['phone']='{}'.format(data['phone'])

            jsongeo_v.append(json_v)

        return json.dumps(jsongeo_v)