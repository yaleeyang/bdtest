import json
import loctag.py.CommonUtil as util


rc = util.ReadConfig('../config.conf')
pj = rc.getItems('ProduceJson')
path = pj['src']
spath = pj['save_path']

geo =[]
'''
把文件中的行数据，转换成前端所能够识别的json格式数据
'''
with open(path,'r') as f:
    lines = f.readlines()
    for line in lines :
#2016-12-19T19:39:30+08:00,217137,22.04,上海,289,上海市,289,虹口区,2466,鲁迅公园,11042,4-优质商圈,虹叶花苑,房地产,121.493566,31.266788
        json_data = {}
        fields = line.rstrip('\n').split(',')
        json_data['terminalid'] = '{}'.format(fields[1])
        json_data['datetime'] = '{}'.format(fields[0])
        json_data['sytime'] = '{}'.format(fields[2])
        json_data['p_name'] = '{}'.format(fields[3])
        json_data['p_code'] = '{}'.format(fields[4])
        json_data['c_name'] = '{}'.format(fields[5])
        json_data['c_code'] = '{}'.format(fields[6])
        json_data['d_name'] = '{}'.format(fields[7])
        json_data['d_code'] = '{}'.format(fields[8])
        json_data['b_name'] = '{}'.format(fields[9])
        json_data['b_code'] = '{}'.format(fields[10])
        json_data['b_type'] = '{}'.format(fields[11])
        json_data['community'] = '{}'.format(fields[12])
        json_data['tag'] = '{}'.format(fields[13])
        json_data['lon'] = '{}'.format(float(fields[14]))
        json_data['lat'] = '{}'.format(float(fields[15]))
        pgs = []
        if not len(fields[16].strip()) ==0:
            polygon = fields[16].rstrip(';').split(';')

            for pg in polygon:
               list_geo = []
               ll = pg.split('  ')
               list_geo.append(float(ll[0]))
               list_geo.append(float(ll[1]))
               pgs.append(list_geo)
        json_data['polygon'] = '{}'.format(pgs)
        json_data['weight'] = '{}'.format(float(fields[17].strip('\n')))
        geo.append(json_data)

with open(spath,'w') as w:
    w.write(json.dumps(geo,ensure_ascii=False))
    w.flush()


