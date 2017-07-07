

import json

import simplejson

from exchange.test import Utils



from exchange.AESCipher import AESCipher


KEY ='f12793c2-7f0f-49d8-8151-0129596ae91b'
IV = '4mA1y7U3xKhXAwB3D4CRqmS6ie88XQmi'



class ImitateYiKa(object):

    def imitateData(self):
        '''
        imitate yika product data
        :return:
        '''
        yk_data = []
        with open('/Users/sunny/Documents/gooddata0/C2016-12-24.csv','r') as files:
            for line in files:
                json_v = {}
                phone = str(line.split(',')[2])
                if(len(phone)==0 or (not phone.isdigit())):
                    continue
                json_v['phone'] = "{}".format(phone)
                yk_data.append(json_v)
        json_yk_data = json.dumps(yk_data)

        print('产生的翼 卡模拟数据：'+str(json_yk_data))
        key = KEY[:16]
        iv=IV[:16]
        encrypt = AESCipher(key,iv)
        encryptData = encrypt.encrypt(json_yk_data)

        Utils.sendRequest('http://0.0.0.0:9875/bd/consumer?apikey=%s'%KEY,encryptData)

    def hadleBMdata(self,BM_data):
        aesc = AESCipher(KEY[:16],IV[:16])
        bm_data = aesc.decrypt(BM_data)
        data = simplejson.loads(bm_data)

        print('翼 卡收到ge平台传过来的数据'+str(data))

#ImitateYiKa().imitateData()


