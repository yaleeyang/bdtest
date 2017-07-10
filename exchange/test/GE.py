import json
import os
import urllib

import simplejson
import time


from exchange.AESCipher import AESCipher

#YIKA
KEY ='f12793c2-7f0f-49d8-8151-0129596ae91b'
IV = '4mA1y7U3xKhXAwB3D4CRqmS6ie88XQmi'

#BirthdayManager
BKEY='cdcd8132-ae1a-4098-80f7-7abdf0313399'
BIV='mABKue3DGqxuNQh6Mj78nUQOOymzDSYF'

class ImitateGE(object):
    '''
    GE platme transform data
    '''
    def decryptYiKaData(self,YK_data,Key):
        '''
        decrypt YIKAData and handle
        :param YK_data:
        :param Key:
        :return:
        '''
        if len(Key) == 0 or len(YK_data)==0:
            return

        crypt = AESCipher(Key[:16],IV[:16])
        if(crypt == None):
            return
        real_yk_data = crypt.decrypt(YK_data)

        #
        yk_data =simplejson.loads(real_yk_data)

        print('ge平台解析后的数据：'+str(yk_data))

        return ImitateGE.encryptYiKaData(self,yk_data)


    def encryptYiKaData(self,yk_data):
        '''
        encrypt data and send BrithdayManager
        :param yk_data:
        :return:
        '''
        crypt = AESCipher(BKEY[:16], BIV[:16])

        json_yk_data=json.dumps(yk_data)

        crypt_yk_data = crypt.encrypt(json_yk_data)

        result = ImitateGE.sendRequest('http://0.0.0.0:9874/bd/producer?apikey=%s'%BKEY,crypt_yk_data)

        return result


    def sendRequest(url, data):
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









