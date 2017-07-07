import json
import random
import simplejson
from exchange.AESCipher import AESCipher
from exchange.test import Utils

BKey="cdcd8132-ae1a-4098-80f7-7abdf0313399"
BIV="mABKue3DGqxuNQh6Mj78nUQOOymzDSYF"


class BirthdayResiveService(object):

    def decryptYiKaData(self,YK_data,Key):
        if len(Key) == 0 or len(YK_data)==0:
            return
        crypt = AESCipher(Key[:16],BIV[:16])
        if(crypt == None):
            return

        #encrypt  GE transmit data
        real_yk_data = crypt.decrypt(YK_data)
        yk_data =simplejson.loads(real_yk_data)#list

        print('生日管家收到ge的数据：'+str(yk_data))
        result = BirthdayResiveService.handl_data(self,yk_data)

        print('生日管家匹配后生成的画像数据：'+result)
        #hadle result send to GE platform
        BirthdayResiveService.encryptBMData(self,result)

    def encryptBMData(self,yk_data):
        crypt = AESCipher(BKey[:16], BIV[:16])
        crypt_yk_data = crypt.encrypt(yk_data)
        Utils.sendRequest('http://0.0.0.0:9875/bd/producer?apikey=%s'%BKey,crypt_yk_data)


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
