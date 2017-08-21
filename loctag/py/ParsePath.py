import json
import os
import utils.CommonUtil as utils
import math
import utils.CommonUtil as util

plng=0
plat=0
geo = []

'''
把数据拆成Echarts的Map所能识别 的数据格式
'''
rc = util.ReadConfig('../config.conf')
pp = rc.getItems('ParsePath')

path = pp['src']
save_path = pp['save_path']

list_file = os.listdir(path)
ldate = None
#默认3轨迹为一文件
default_num=3

def saveFile(save_path,geo):
    o = open(save_path, 'w')
    json_data = json.dumps(geo, ensure_ascii=False)
    o.write(json_data)
    o.flush()
    o.close()

for fn in list_file:
    val = []
    with open(path+fn,'r') as f:
        lines = f.readlines()
        for i in range(0,len(lines),2):
            fields = lines[i].strip('\n').split(',')
            lng = fields[4]
            lat = fields[3]
            if lng.isalpha()==True or lat.isalpha()==True :
                continue
            date = fields[0][:fields[0].index('T')]

            # if plng==0 and plat==0:
            if not ldate == date:
                if not ldate==None:
                    if len(val)>5:
                        geo.append(val)
                    val=[]
                    plng = 0
                    plat = 0
                ldate=date

            lnglats = utils.wgs84_to_bd09(float(lng),float(lat))
            lng = int(lnglats[0]*1e4)
            lat = int(lnglats[1]*1e4)
            lng_step = lng - plng
            lat_step = lat - plat
            #防止轨迹为直线
            if not len(val)== 0 and abs(lng_step)>90:
                continue
            if not len(val) == 0 and abs(lat_step)>200:
                continue
            val.append(lng_step)
            val.append(lat_step)
            plng=lng
            plat=lat
        if len(val) > 5:
            geo.append(val)

        #取得文件的个数
        file_num = math.ceil(len(geo) / default_num)
        for i in range(file_num) :
            #根据文件个数和默认的轨迹数确定每个文件的轨迹数
            data = geo[i*default_num:(i+1)*default_num]
            saveFile(save_path+'trajectory%s.json'%i,data)

        info={}
        #文件个数反馈给前端页面
        info['file_num'] = '{}'.format(file_num)
        saveFile(save_path+'tinfo.json', info)




