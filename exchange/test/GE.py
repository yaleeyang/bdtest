import json
import os

import simplejson
import time
from exchange.test import  Utils

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

        ImitateGE.encryptYiKaData(self,yk_data)


    def encryptYiKaData(self,yk_data):
        '''
        encrypt data and send BrithdayManager
        :param yk_data:
        :return:
        '''
        crypt = AESCipher(BKEY[:16], BIV[:16])

        json_yk_data=json.dumps(yk_data)

        crypt_yk_data = crypt.encrypt(json_yk_data)

        Utils.sendRequest('http://0.0.0.0:9874/bd/producer?apikey=%s'%BKEY,crypt_yk_data)


    def decryptBirthdayData(self,BM_data,BKey):
        '''
        decrypt BirthdayData and handle
        :param BM_data:
        :param BKey:
        :return:
        '''
        if len(BKey) == 0 or len(BM_data)==0:
            return

        crypt = AESCipher(BKey[:16],BIV[:16])
        if(crypt == None):
            return
        real_bm_data = crypt.decrypt(BM_data)

        #
        bm_data =simplejson.loads(real_bm_data)

        ImitateGE.encryptBirthdayData(self,bm_data)

    def encryptBirthdayData(self,bm_data):
        '''
        encrypt data and send YIKA
        :param bm_data:
        :return:
        '''
        crypt = AESCipher(KEY[:16], IV[:16])

        json_bm_data = json.dumps(bm_data)

        crypt_bm_data = crypt.encrypt(json_bm_data)

        print('ge平台接受生日管家解析后的数据'+str(crypt_bm_data,'utf-8'))

        Utils.sendRequest('http://0.0.0.0:9873/bd/consumer?apikey=%s' % BKEY, crypt_bm_data)









