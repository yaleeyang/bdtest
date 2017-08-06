--如果表不存在，创建该表，用来存储清洗完的数据
CREATE TABLE IF NOT EXISTS sunny.vehicle_trajectory(
datetime string,
terminalId string,
time  string,
lat string,
lon string,
direction string,
speed string
)
row format delimited fields terminated by ',';

--修改表的数据存储位置
alter table sunny.vehicle_trajectory set location '${hiveconf:DATA_PATH}';

--求出车辆停留时间top100

drop table sunny.vehicle_trajectory_tmp;
--创建vehicle_trajectory临时表，多了个row_num字段

--防止下面时间表创建失败
drop table sunny.sql_runtime;
--创建统计sql运行时间表，并且记录该sql开始时间
create table sunny.sql_runtime as select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,'create-vehicle_trajectory_tmp' as name,unix_timestamp() as stime,0 as etime,0 as used_time;
--get 数据的行号
create table if not exists sunny.vehicle_trajectory_tmp as select datetime,terminalid,time,lat,lon,direction,speed,row_number() over(order by time asc) as row_num  from sunny.vehicle_trajectory ;
--记录该sql结束时间，并计算使用的时间（秒）
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,name,stime,unix_timestamp() etime,(unix_timestamp()-stime) used_time from sunny.sql_runtime where name ='create-vehicle_trajectory_tmp';

drop table sunny.staytime_top100;

--统计top100的运行时间
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,'create-staytime_top100' name,unix_timestamp() as stime,0 etime,0 used_time from sunny.sql_runtime limit 1;
--停留时间top n
create table if not exists sunny.staytime_top100 as SELECT b.datetime,b.terminalid,b.time,b.lat,b.lon,b.direction,b.speed,(coalesce(cast(a.time as bigint) - cast(b.time as bigint))) sytime from sunny.vehicle_trajectory_tmp a join sunny.vehicle_trajectory_tmp b where a.terminalId=b.terminalId and a.row_num = b.row_num+1 order by sytime desc limit 100;
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,name,stime,unix_timestamp() etime,(unix_timestamp()-stime) used_time from sunny.sql_runtime where name ='create-staytime_top100';


--添加自定义函数
add jar hdfs://sandbox.hortonworks.com:8020/esri/esri-geometry-api.jar;
add jar hdfs://sandbox.hortonworks.com:8020/esri/spatial-sdk-hive-1.1.1-SNAPSHOT.jar;
add jar hdfs://sandbox.hortonworks.com:8020/esri/spatial-sdk-json-1.1.1-SNAPSHOT.jar;

add jar hdfs://sandbox.hortonworks.com:8020/user/sunny/jars/geo.jar;

create temporary function st_geomfromtext as 'com.esri.hadoop.hive.ST_GeomFromText';

create temporary function st_geometrytype as 'com.esri.hadoop.hive.ST_GeometryType';

create temporary function st_point as 'com.esri.hadoop.hive.ST_Point';

create temporary function st_asjson as 'com.esri.hadoop.hive.ST_AsJson';

create temporary function st_asbinary as 'com.esri.hadoop.hive.ST_AsBinary';

create temporary function st_astext as 'com.esri.hadoop.hive.ST_AsText';

create temporary function st_intersects as 'com.esri.hadoop.hive.ST_Intersects';

create temporary function st_x as 'com.esri.hadoop.hive.ST_X';

create temporary function st_y as 'com.esri.hadoop.hive.ST_Y';

create temporary function st_srid as 'com.esri.hadoop.hive.ST_SRID';

create temporary function st_linestring as 'com.esri.hadoop.hive.ST_LineString';

create temporary function st_polygon as 'com.esri.hadoop.hive.ST_Polygon';

create temporary function st_pointn as 'com.esri.hadoop.hive.ST_PointN';

create temporary function st_startpoint as 'com.esri.hadoop.hive.ST_StartPoint';

create temporary function st_endpoint as 'com.esri.hadoop.hive.ST_EndPoint';

create temporary function st_numpoints as 'com.esri.hadoop.hive.ST_NumPoints';

create temporary function second2hm as 'com.tuqu.sunny.hive.SecondTransform';

drop table sunny.province_geo_tmp ;

--统计点匹配省的sql运行时间
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,'create-province_geo_tmp' name,unix_timestamp() as stime,0 etime,0 used_time from sunny.sql_runtime limit 1;
--确定每个点匹配的面（省份）
create table if not exists sunny.province_geo_tmp as
select rs.terminalid,rs.sytime,rs.p_name,rs.p_code,rs.lon,rs.lat from (
select  po.terminalid,po.sytime,go.area_name p_name,go.area_code p_code,po.lon,po.lat ,st_intersects(st_point(po.lon,po.lat), st_polygon(go.lnlts)) flag from (
select datetime,terminalid,time,lon,lat,direction,speed,sytime 
from sunny.staytime_top100) po,
(
select area_name,area_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sunny.sh_gd_province_tmp
) go) rs
where flag =true;
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,name,stime,unix_timestamp() etime,(unix_timestamp()-stime) used_time from sunny.sql_runtime where name ='create-province_geo_tmp';


drop table sunny.city_geo_tmp;

--统计省－市的sql运行时间
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,'create-city_geo_tmp' name,unix_timestamp() as stime,0 etime,0 used_time from sunny.sql_runtime limit 1;
--根据省份code确定每个点匹配更小面的位置（省－市）
create table if not exists sunny.city_geo_tmp as
select cp.c_name,cp.c_code,cp.lon,cp.lat from (
select city.area_name c_name,city.area_code c_code,pro.lon,pro.lat,
st_intersects(st_point(pro.lon,pro.lat), st_polygon(city.lnlts)) flag
from 
(select area_name,area_code,province_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sunny.sh_gd_city_tmp) city 
join
(select p_code,lon,lat from sunny.province_geo_tmp)  pro
where city.province_code = pro.p_code ) cp
where cp.flag=true;
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,name,stime,unix_timestamp() etime,(unix_timestamp()-stime) used_time from sunny.sql_runtime where name ='create-city_geo_tmp';



drop table sunny.district_geo_tmp;

--统计市－区sql运行时间
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,'create-district_geo_tmp' name,unix_timestamp() as stime,0 etime,0 used_time from sunny.sql_runtime limit 1;
--根据城市code确定各个点匹配更小的位置（市－区）
create table if not exists sunny.district_geo_tmp as
select cpd.d_name,cpd.d_code,cpd.lon,cpd.lat from (
select dt.area_name d_name,dt.area_code d_code,poc.lon,poc.lat,
st_intersects(st_point(poc.lon,poc.lat), st_polygon(dt.lnlts)) flag
from 
(select area_name,area_code,city_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sunny.map_gd_district_tmp)  dt
join
(select c_code,lon,lat from sunny.city_geo_tmp) poc
where poc.c_code = dt.city_code
) cpd
where cpd.flag =true;
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,name,stime,unix_timestamp() etime,(unix_timestamp()-stime) used_time from sunny.sql_runtime where name ='create-district_geo_tmp';


drop table sunny.business_geo_tmp;

--统计区－商圈的sql运行时间
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,'create-business_geo_tmp' name,unix_timestamp() as stime,0 etime,0 used_time from sunny.sql_runtime limit 1;
--根据城市code确定各个点匹配更小的位置（区－商圈）
create table if not exists sunny.business_geo_tmp as
select sq.b_name,sq.b_code,sq.b_type,sq.lon,sq.lat from(
select bn.area_name b_name,bn.area_code b_code,bn.business_type b_type,pcd.lon,pcd.lat, 
st_intersects(st_point(pcd.lon,pcd.lat),st_polygon(cast(bn.lnlts as string))) flag
from 
(select area_name,business_type,area_code,district_code,concat("polygon((",regexp_replace(regexp_replace(business_geo,","," "),"\;",","),"))") lnlts from sunny.map_gd_business_tmp) bn 
join
(select d_name,d_code,lon,lat from sunny.district_geo_tmp) pcd where pcd.d_code=bn.district_code
) sq where sq.flag=true;
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,name,stime,unix_timestamp() etime,(unix_timestamp()-stime) used_time from sunny.sql_runtime where name ='create-business_geo_tmp';


drop table sunny.trajectory_cloud;

--统计省－市－区－商圈的sql运行时间
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,'create-trajectory_cloud' name,unix_timestamp() as stime,0 etime,0 used_time from sunny.sql_runtime limit 1;
--多表联合
create table if not exists sunny.trajectory_cloud  as select terminalid,sytime,p_name,p_code,c_name,c_code,d_name,d_code, b_name,b_code,b_type,cy.lon,cy.lat from 
(select terminalid,sytime,p_name,p_code,lon,lat from sunny.province_geo_tmp) pn 
left outer join 
(select c_name,c_code,lon,lat from sunny.city_geo_tmp) cy on (pn.lon = cy.lon and pn.lat = cy.lat)
left outer join 
(select d_name,d_code,lon,lat from sunny.district_geo_tmp) dt 
on (cy.lon = dt.lon and cy.lat = dt.lat)
left outer join 
(select b_name,b_code,b_type,lon,lat from sunny.business_geo_tmp) bn
on (dt.lon = bn.lon and dt.lat = bn.lat);
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,name,stime,unix_timestamp() etime,(unix_timestamp()-stime) used_time from sunny.sql_runtime where name ='create-trajectory_cloud';

--删除临时表
drop table sunny.vehicle_trajectory_tmp;

drop table sunny.staytime_top100;

drop table sunny.province_geo_tmp ;

drop table sunny.city_geo_tmp;

drop table sunny.district_geo_tmp;

drop table sunny.business_geo_tmp;

--统计导出trajectory_cloud的sql运行时间
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,'export-trajectory_cloud' name,unix_timestamp() as stime,0 etime,0 used_time from sunny.sql_runtime limit 1;
--导出trajectory_cloud
insert overwrite local directory '/opt/sunny/hive-export/original/' row format delimited fields  terminated by ',' select * from sunny.trajectory_cloud;
insert into table sunny.sql_runtime select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') datetime,name,stime,unix_timestamp() etime,(unix_timestamp()-stime) used_time from sunny.sql_runtime where name ='export-trajectory_cloud';