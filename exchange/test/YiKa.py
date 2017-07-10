import json
from exchange.AESCipher import AESCipher


KEY ='f12793c2-7f0f-49d8-8151-0129596ae91b'
IV = '4mA1y7U3xKhXAwB3D4CRqmS6ie88XQmi'



class ImitateYiKa(object):

    def imitateData(self):
        '''
        imitate yika product data
        生成加密后的yika数据，然后把加密后的数据使用postman来send
        :return:
        '''
        yk_data = []
        with open('./C2016-12-24.csv','r') as files:
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
ImitateYiKa().imitateData()


