import os
import sys
import loctag.py.getXQ as getXQ
import time

'''
python python文件 清洗文件 hdfs上的路径 hql文件
输入路径：绝对路径（file:// 为本地路径，不写默认为hdfs路径）
输出路径：hdfs的路径
hql路径：hql文件所在的路径
'''
hdfspath = 'hdfs://sandbox.hortonworks.com:8020/'
startTime = time.time() #开始时间
#0:脚ben名，1－n:参数
if len(sys.argv)<4:
    print("error:input path and output path and hql path is exists!")
    sys.exit(1)
else:
    entry = sys.argv[1] #input
    data = sys.argv[2]   #output
    hql = sys.argv[3]   #hql

    code = os.system('yarn jar /opt/sunny/Jars/geo.jar com.tuqu.sunny.mr.ClearTrajectoryData %s  %s'%(entry,data))
    print('run yarn Time taken: %s seconds'%(time.time()-startTime))
    #上面命令执行成功
    if code ==0:
        if not data.startswith(hdfspath):
            data = hdfspath+data

        #指定mapreduce清洗后数据保存的位置
        #指定hql文件所在的位置(default:/opt/sunny/hql/exec.hql)
        hso_time = time.time()
        cmd ='hive -hiveconf DATA_PATH=%s -f %s'%(data,hql)
        Hcode = os.system(cmd)
        print('run frist hive script Time taken: %s seconds'%(time.time()-hso_time))

        if Hcode==0:
            ptime = time.time()
            getXQ.getLngLat()
            print('run python script Time taken: %s seconds'%(time.time()-ptime))

            hst_time =time.time()
            code = os.system('hive -f /opt/sunny/hql/xq.hql')
            print('run second hive script Time taken: %s seconds'%(time.time()-hst_time))
            if code ==0:
                print('xq.hql success!')
            else :
                print('xq.hql fail!')
        else:
            print(hql+' fail!')

endTme = time.time()#结束时间
print('Time taken: %s seconds' %(endTme-startTime))


#hive -hiveconf DATA_PATH=hdfs:///tmp/output -f /opt/sunny/exec.hql

#python /opt/sunny/py/PyUseHive.py file:///opt/sunny/data-hive/12all.csv /tmp/output hql/exec.hql
