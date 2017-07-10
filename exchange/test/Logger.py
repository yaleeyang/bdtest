import os
import time

path='./'
suffle='.log'
def log(line):
    '''
    该函数用来记录每天交换的数据
    :param line:
    :return:
    '''
    #以每天的日期对该log文件进行命名
    filename = time.strftime('%Y-%m-%d',time.localtime())+suffle
    #判断该文件是否存在
    if not os.path.exists(path+filename):
        os.system('touch '+path+filename)

    try:
        #以追加和读的模式打开文件
        file = open(path+filename,'a+')
        file.write(line)
        file.newlines
    except IOError as e:
        print('file execute error:'+e)
    finally:
        if not file==None:
            file.close()