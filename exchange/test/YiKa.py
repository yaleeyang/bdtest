import json

from exchange.AESCipher import AESCipher
from exchange.Utils import ReadConfig



rc = ReadConfig('../ge.conf')
consumer = rc.getItems('consumer')
producer = rc.getItems('producer')

IV =consumer['key']
KEY = consumer['secrete']
class ImitateYiKa(object):

    def imitateData(self):
        '''
        imitate yika product data
        生成加密后的yika数据，然后把加密后的数据使用postman来send
        :return:
        '''
        yk_data = test = [{
            'data':{
    "age": {
        "95+": 0.045335142539415,
        "95-": 0.95466485746059
    },
    "fn": "张",
    "fn_available": 0.999,
    "nm": "三",
    "nm_available": 0.999,
    "organization": [],
    "other_name": [],
    "recorded_count": 1,
    "sex": [
        0.99,
        0.01
    ],
    "title": [],
    "code": "ok"
}
        }]
        # with open('./T2016-12-24.csv','r') as files:
        #     for line in files:
        #         json_v = {}
        #         phone = str(line.split(',')[2])
        #         if(len(phone)==0 or (not phone.isdigit())):
        #             continue
        #         json_v['phone'] = "{}".format(phone)
        #         yk_data.append(json_v)
        json_yk_data = json.dumps(yk_data[0]['data'])
        print(type(json_yk_data))
        print('产生的翼 卡模拟数据：'+str(json_yk_data))
        # key = KEY[:16]
        # iv=IV[:16]
        iv = 'cdcd8132-ae1a-4098-80f7-7abdf0313399'[:16]
        key = 'mABKue3DGqxuNQh6Mj78nUQOOymzDSYF'[:16]
        print(str(yk_data[0]['data']))

        encrypt = AESCipher(key,iv)
        encryptData = encrypt.encrypt(json_yk_data)
        print(encryptData)

    def Test(self):
        key = producer['secrete'][:16]
        iv = producer['key'][:16]
        print(key)
        print(iv)
        test = [{
            'data':{
                #'phone':'13162090723'
                'phone':'13817762644'
            }
        }]
        data = json.dumps(test[0]['data'])
        #encrypts = AESCipher(key.strip(), iv.strip())
        encrypts = AESCipher(KEY[:16].strip(), IV[:16].strip())
        encryptData = encrypts.encrypt(data)#加密必须是字符串不能是字节
        print(encryptData)#解密字符串和字节都行
                                            # 发送的数据都是字节
        print("---------------------------------------------")

        d = encrypts.decrypt(encryptData)
        en = d.decode('unicode_escape')
        print(en)
        print(d)
#ImitateYiKa().imitateData()
ImitateYiKa().Test()


