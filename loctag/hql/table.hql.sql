use sunny;

create external table if not exists map_gd_province_tmp(
area_type string,
geo string,
area_name string,
area_code string
)
row format delimited fields terminated by '\t'
stored as textfile;

create external table if not exists map_gd_city_tmp(
area_type string,
geo string,
area_name string,
area_code string,
province_code string
)
row format delimited fields terminated by '\t'
stored as textfile;

create external table if not exists map_gd_district_tmp2(
area_type string,
geo string,
area_name string,
area_code string,
city_code string
)
row format delimited fields terminated by '\t'
stored as textfile;

create external table if not exists map_gd_business_tmp(
area_type string,
geo string,
area_name string,
area_code string,
district_code string,
business_type string,
business_geo  string
)
row format delimited fields terminated by '\t'
stored as textfile;

create external table if not exists map_gd_district(
area_code string,
area_name string,
city_code string,
geo string,
area_type string
)
row format delimited fields terminated by '\t'
stored as orc tblproperties('orc.compression'='snappy');

create external table if not exists map_gd_business(
area_code string,
area_name string,
geo string,
area_type string,
district_code string,
business_type string,
business_geo  string
)
row format delimited fields terminated by '\t'
stored as ORC TBLPROPERTIES('ORC.COMPRESSION'='SNAPPY');

#往orc文件格式中插入数据
INSERT INTO TABLE MAP_GD_PROVINCE SELECT AREA_CODE,AREA_NAME,GEO,AREA_TYPE FROM MAP_GD_PROVINCE_TMP;

INSERT INTO TABLE MAP_GD_CITY SELECT AREA_CODE,AREA_NAME,PROVINCE_CODE,GEO,AREA_TYPE FROM MAP_GD_CITY_TMP;

INSERT INTO TABLE MAP_GD_DISTRICT SELECT AREA_CODE,AREA_NAME,CITY_CODE,GEO,AREA_TYPE FROM MAP_GD_DISTRICT_TMP;

INSERT INTO TABLE MAP_GD_BUSINESS SELECT AREA_CODE,AREA_NAME,GEO,AREA_TYPE,DISTRICT_CODE,BUSINESS_TYPE,BUSINESS_GEO FROM MAP_GD_BUSINESS_TMP;


#轨迹数据
use sunny;
CREATE TABLE IF NOT EXISTS sunny.vehicle_trajectory(
datetime string,
terminalId string,
time  string,
lat string,
lon string,
direction string,
speed string
);

#get 数据的行号
create table IF NOT EXISTS  sunny.vehicle_trajectory_tmp select *,row_number() over(order by time asc) as row_num  from sunny.vehicle_trajectory;

#相邻数据时间差
SELECT coalesce(cast(a.time as bigint) - cast(b.time as bigint)) from sunny.vehicle_trajectory_tmp a join sunny.vehicle_trajectory_tmp b where a.row_num = b.row_num+1;

#停留时间top n
create  table staytime_top100b as SELECT coalesce(cast(a.time as bigint) - cast(b.time as bigint)) from sunny.vehicle_trajectory_tmp a join sunny.vehicle_trajectory_tmp b where a.terminalId=b.terminalId and a.row_num = b.row_num+1  order by sytime desc limit 100;
