import os
import time
from configparser import ConfigParser

path='./log/'
suffle='.log'
def log(line):
    '''
    该函数用来记录每天交换的数据
    :param line:
    :return:
    '''
    #以每天的日期对该log文件进行命名
    filename = time.strftime('%Y-%m-%d',time.localtime())+suffle
    file=None
    #判断目录是否存在
    if not os.path.exists(path):
        os.mkdir(path)

    #判断该文件是否存在
    if not os.path.exists(path+filename):
        os.system('touch '+path+filename)

    try:
        #以追加和读的模式打开文件
        file = open(path+filename,'a+')
        file.write(line+'\r\n')
    except IOError as e:
        print('file execute error:'+e)
    finally:
        if not file==None:
            file.close()

class ReadConfig(object):
    def __init__(self,filename=None):
        if filename is None or len(filename.strip()) == 0:
            return
        self.filename = filename

    def getItems(self,section):
        '''
        :param section:type is str
        :return: dict_items type is dict
        '''

        if not isinstance(section,str) and len(section.strip()) == 0:
            return

        cp = ConfigParser()
        # load file
        cp.read(self.filename)

        sections = cp.items(section.strip())

        dict_items={}
        for st in sections:
            dict_items[st[0]]=st[1]

        return dict_items