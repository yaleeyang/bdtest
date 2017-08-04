
CREATE TABLE IF NOT EXISTS sunny.trajectory_cloud_tmp(
terminalid string,
sytime float,         
p_name string,       
p_code string,        
c_name string,        
c_code string,        
d_name string,        
d_code string,        
b_name string,        
b_code string,        
b_type string,        
lon string,        
lat string,
community string
)
row format delimited fields terminated by ',';

--加载数据
load data local inpath '/opt/sunny/hive-export/match/trajectory_cloud.csv' overwrite into table  sunny.trajectory_cloud_tmp;

drop table trajectory_cloud_status;

--根据b_code,community去重
create table if not exists trajectory_cloud_status as select t.terminalid,t.sytime,t.p_name,t.p_code,t.c_name,t.c_code,t.d_name,t.d_code,t.b_name,t.b_code,t.b_type,t.community,t.lon,t.lat from(select *,row_number() over(distribute by b_code,community sort by sytime desc) num from sunny.trajectory_cloud_tmp) t where t.num = 1 order by sytime desc;

