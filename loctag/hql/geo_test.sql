
#遍历geo列
select c_geo from sh_gd_city_tmp lateral view explode(split(geo,',')) geo as c_geo limit 1;

select explode(split('1,2,3,4,5,6',','))

select regexp_replace(geo,";",",") from sh_gd_city_tmp limit 1;

select split(geo,'\;') from sh_gd_city_tmp limit 2; 

create table if not exists staytime_top100_tmp as
#------------------------


#确定每个点匹配的面（省份）
create table test_geo_tmp as
select rs.* from (
select  po.terminalid,po.sytime,go.area_code,case when go.area_name is not null then go.area_name else '国外' end,st_intersects(st_point(po.lon,po.lat), st_polygon(go.lnlts)) flag from (
select datetime,terminalid,time,lon,lat,direction,speed,sytime 
from staytime_top100) po,
(
select area_name,area_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sh_gd_province_tmp
) go) rs
where rs.flag =true;

＃根据省份code确定每个点匹配更小面的位置（省－市）
select * from (
select pro.terminalid,pro.sytime,pro.area_code,pro.area_name,city.area_name,city.area_code,
st_intersects(st_point(pro.lon,pro.lat), st_polygon(city.lnlts)) flag
 from 
(select area_name,area_code,province_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sh_gd_city_tmp) city 
join
(select * from (
select  po.terminalid,po.sytime,po.lon,po.lat,go.area_code,go.area_name,st_intersects(st_point(po.lon,po.lat), st_polygon(go.lnlts)) flag from (
select datetime,terminalid,time,lon,lat,direction,speed,sytime 
from staytime_top100) po,
(
select area_name,area_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sh_gd_province_tmp
) go) rs
where rs.flag =true) pro
where city.province_code = pro.area_code ) cp
where cp.flag=true;

#根据城市code确定各个点匹配更小的位置（市－区）
#1.直辖市相当于省，市相当于区
select * from (
select poc.terminalid,poc.sytime,poc.p_code,poc.p_name,poc.c_code,poc.c_name,dt.area_name,dt.area_code,
st_intersects(st_point(poc.lon,poc.lat), st_polygon(dt.lnlts)) flag
 from 
(select area_name,area_code,city_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from map_gd_district_tmp)  dt
join
(select * from (
select pro.terminalid,pro.sytime,pro.area_code p_code,pro.area_name p_name,pro.lon,pro.lat,city.area_name c_name,city.area_code c_code,
st_intersects(st_point(pro.lon,pro.lat), st_polygon(city.lnlts)) flag
 from 
(select area_name,area_code,province_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sh_gd_city_tmp) city 
join
(select * from (
select  po.terminalid,po.sytime,po.lon,po.lat,go.area_code,go.area_name,st_intersects(st_point(po.lon,po.lat), st_polygon(go.lnlts)) flag from (
select datetime,terminalid,time,lon,lat,direction,speed,sytime 
from staytime_top100) po,
(select area_name,area_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sh_gd_province_tmp
) go) rs
where rs.flag =true limit 1) pro
where city.province_code = pro.area_code ) cp
where cp.flag=true
) poc
where poc.c_code = dt.city_code
) cpd
where cpd.flag =true

#根据城市code确定各个点匹配更小的位置（区－商圈）
select * from (
select pcd.terminalid,pcd.sytime,pcd.p_code,pcd.p_name,pcd.c_code,pcd.c_name,pcd.d_name,bn.area_name,bn.business_type,bn.area_code,bn.district_code,
st_intersects(st_point(pcd.lon,pcd.lat), st_polygon(bn.lnlts)) flag
 from 
(select area_name,business_type,area_code,district_code,concat("polygon((",regexp_replace(regexp_replace(business_geo,","," "),"\;",","),"))") lnlts 
from map_gd_business) bn 
join
(select * from (
select poc.terminalid,poc.sytime,poc.p_code,poc.p_name,poc.c_code,poc.c_name,dt.area_name d_name,dt.area_code,poc.lon,poc.lat,
st_intersects(st_point(poc.lon,poc.lat), st_polygon(dt.lnlts)) flag
 from 
(select area_name,area_code,city_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from map_gd_district_tmp)  dt
join
(select * from (
select pro.terminalid,pro.sytime,pro.area_code p_code,pro.area_name p_name,pro.lon,pro.lat,city.area_name c_name,city.area_code c_code,
st_intersects(st_point(pro.lon,pro.lat), st_polygon(city.lnlts)) flag
 from 
(select area_name,area_code,province_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sh_gd_city_tmp) city 
join
(select * from (
select  po.terminalid,po.sytime,po.lon,po.lat,go.area_code,go.area_name,st_intersects(st_point(po.lon,po.lat), st_polygon(go.lnlts)) flag from (
select datetime,terminalid,time,lon,lat,direction,speed,sytime 
from staytime_top100) po,
(select area_name,area_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sh_gd_province_tmp
) go) rs
where rs.flag =true ) pro
where city.province_code = pro.area_code ) cp
where cp.flag=true
) poc
where poc.c_code = dt.city_code
) cpd
where cpd.flag =true) pcd where pcd.area_code=bn.district_code
) sq where sq.flag=true;






#-------------------------------Test-------------------------------


在select使用聚会函数(只能是聚合函数，不能有字段)，不使用group by，必须设置
set hive.map.aggr=true;



CREATE TABLE IF NOT EXISTS area_match_tmp(
terminalid string comment '用户ID',
sytime bigint comment '停留时间',
province_name string comment '省份或直辖市',
province_code string comment '省ID',
city_name string comment '市名',
city_code string comment '市ID',
district_name string comment '区名',
district_code string comment '区ID',
bussiness_name string comment '商圈名',
bussiness_code string comment '商圈ID',
bussiness_type string comment '商圈类型',
flag boolean comment '是否匹配'
)
row format delimited fields terminated by '\t';



#确定每个点匹配的面（省份）
insert overwrite table province_geo_tmp 
select rs.terminalid,rs.sytime,rs.p_name,rs.p_code,rs.lon,rs.lat from (
select  po.terminalid,po.sytime,go.area_name p_name,go.area_code p_code,po.lon,po.lat ,st_intersects(st_point(po.lon,po.lat), st_polygon(go.lnlts)) flag from (
select datetime,terminalid,time,lon,lat,direction,speed,sytime 
from staytime_top100) po,
(
select area_name,area_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sh_gd_province_tmp
) go) rs
where flag =true;


select terminalid,sytime,p_name,p_code,lon,lat from province_geo_tmp;


＃根据省份code确定每个点匹配更小面的位置（省－市）
insert overwrite table city_geo_tmp 
select cp.c_name,cp.c_code,cp.lon,cp.lat from (
select city.area_name c_name,city.area_code c_code,pro.lon,pro.lat,
st_intersects(st_point(pro.lon,pro.lat), st_polygon(city.lnlts)) flag
 from 
(select area_name,area_code,province_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from sh_gd_city_tmp) city 
join
(select p_code,lon,lat from province_geo_tmp)  pro
where city.province_code = pro.p_code ) cp
where cp.flag=true;


select c_name,c_code,lon,lat from city_geo_tmp;

#根据城市code确定各个点匹配更小的位置（市－区）
insert overwrite table district_geo_tmp 
select cpd.d_name,cpd.d_code,cpd.lon,cpd.lat from (
select dt.area_name d_name,dt.area_code d_code,poc.lon,poc.lat,
st_intersects(st_point(poc.lon,poc.lat), st_polygon(dt.lnlts)) flag
 from 
(select area_name,area_code,city_code,concat("polygon((",regexp_replace(regexp_replace(geo,","," "),"\;",","),"))") lnlts 
from map_gd_district_tmp)  dt
join
(select c_code,lon,lat from city_geo_tmp) poc
where poc.c_code = dt.city_code
) cpd
where cpd.flag =true


select d_name,d_code,lon,lat from district_geo_tmp;

#根据城市code确定各个点匹配更小的位置（区－商圈）
insert overwrite table business_geo_tmp 
select sq.b_name,sq.b_code,sq.b_type,sq.lon,sq.lat from(
select bn.area_name b_name,bn.area_code b_code,bn.business_type b_type,pcd.lon,pcd.lat, 
st_intersects(st_point(pcd.lon,pcd.lat),st_polygon(cast(bn.lnlts as string))) flag
 from 
(select area_name,business_type,area_code,district_code,concat("polygon((",regexp_replace(regexp_replace(business_geo,","," "),"\;",","),"))") lnlts from map_gd_business_tmp) bn 
join
(select d_name,d_code,lon,lat from district_geo_tmp) pcd where pcd.d_code=bn.district_code
) sq where sq.flag=true;

select b_name,b_code,b_type,lon,lat from business_geo_tmp;


##汇总
select terminalid,sytime,p_name,p_code,lon,lat from province_geo_tmp;

select c_name,c_code,lon,lat from city_geo_tmp;

select d_name,d_code,lon,lat from district_geo_tmp;

select b_name,b_code,b_type,lon,lat from business_geo_tmp;

##多表联合
select terminalid,sytime,p_name,p_code,c_name,c_code,d_name,d_code, b_name,b_code,b_type,bn.lon,bn.lat from 
(select terminalid,sytime,p_name,p_code,lon,lat from province_geo_tmp) pn 
left outer join 
(select c_name,c_code,lon,lat from city_geo_tmp) cy on (concat_ws(',',pn.lon,pn.lat) = concat_ws(',',cy.lon,cy.lat)) 
left outer join 
(select d_name,d_code,lon,lat from district_geo_tmp) dt 
on concat_ws(',',cy.lon,cy.lat)=concat_ws(',',dt.lon,dt.lat) 
left outer join 
(select b_name,b_code,b_type,lon,lat from business_geo_tmp) bn
on concat_ws(',',dt.lon,dt.lat) =concat_ws(',',bn.lon,bn.lat);


insert into table staytime_top100 values("2016-12-20T07:55:06+08:00","123455","32423","23.137961683293","113.27269822472","12.4","65.3",1),("2016-12-20T07:55:06+08:00","888888","32423","31.299496","121.695117","12.4","65.3",1);
