
import loctag.py.CommonUtil as util
'''
主要把hive处理后的数据格式WGS84-》任何经纬度格式（）
'''

class gcj02(object):
    """
        WGS84转GCJ02(火星坐标系)
        :param lng:WGS84坐标系的经度
        :param lat:WGS84坐标系的纬度
        :return:
    """
    def __init__(self,lng,lat):
        self.lng = lng
        self.lat = lat

    def transform(self):
        return util.wgs84_to_gcj02(self.lng,self.lat)

class bd09(object):
    """
        WGS84转BD09(百度坐标系)
        :param lng:WGS84坐标系的经度
        :param lat:WGS84坐标系的纬度
        :return:
    """
    def __init__(self,lng,lat):
        self.lng = lng
        self.lat = lat

    def transform(self):
        return util.wgs84_to_bd09(self.lng,self.lat)

class adapter(object):
    '''
    适配器
    '''
    def __init__(self, obj, adapted_methods):
        self.obj = obj
        self.__dict__.update(adapted_methods)

#Test
# def run():
#     lng = 121.493995
#     lat = 31.277239
#     g = gcj02(lng,lat)
#     obj = adapter(g,dict(execute=g.transform))
#     print(obj.execute())
