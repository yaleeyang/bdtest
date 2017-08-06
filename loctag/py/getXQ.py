import json
import math
import os
from urllib import request,parse
#从hive导出的数据存放目录
file_path='/opt/sunny/hive-export/original/000000_0'
save_path='/opt/sunny/hive-export/match/'
file_name='trajectory_cloud.csv'

ak = 'CKzzl0ODAd4kBl0TzXTl59F9Ne9Bzrgk'
x_pi = 3.14159265358979324 * 3000.0 / 180.0
url = 'http://api.map.baidu.com/geocoder/v2/?'

def gcj02_to_bd09(lng, lat):
    """
    火星坐标系(GCJ-02)转百度坐标系(BD-09)
    谷歌、高德——>百度
    :param lng:火星坐标经度
    :param lat:火星坐标纬度
    :return:
    """
    z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * x_pi)
    theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * x_pi)
    bd_lng = z * math.cos(theta) + 0.0065
    bd_lat = z * math.sin(theta) + 0.006
    return [bd_lng, bd_lat]

def sendRequest(lng,lat):
    '''
    请求百度的API
    :param lng: 经度
    :param lat: 维度
    :return: 类型是字典
    '''
    args={
        'callback':'',
        'location':"%s,%s"%(lat,lng),
        'output' :'json',
        'pois':'1',
        'ak':ak
    }
    url_args = parse.urlencode(args)
    req = request.Request(url+url_args)
    resp = request.urlopen(req)
    result = resp.read().decode('utf-8')
    #把返回的结果转换为dict
    json_result = json.loads(result)
    return json_result

def getLngLat():

    if not os.path.exists(file_path):
        raise IOError(file_path+" is not exists ")

    if not os.path.exists(save_path):
        os.system('mkdir %s'%save_path)
    save = open(save_path+file_name, 'w+')
    with open(file_path,'r+') as file:
        lines = file.readlines()
        for line in lines:

            line = line.rstrip('\n')
            fields = line.split(',')
            if(len(fields)<13):
                continue
            lng = fields[11].strip()
            lat = fields[12].strip()
            #高德坐标转百度
            lnglat = gcj02_to_bd09(float(lng),float(lat))
            #获取位置信息
            addr_info = sendRequest(lnglat[0],lnglat[1])

            addr = addr_info['result']['pois'][0].get('name','Null')
            save.write(line+','+addr+'\n')
    save.close()

#insert overwrite local directory '/opt/sunny/hive-export/original/' row format delimited fields  terminated by ',' select * from sunny.trajectory_cloud;
