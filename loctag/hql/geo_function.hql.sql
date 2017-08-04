
add jar hdfs://sandbox.hortonworks.com:8020/esri/esri-geometry-api.jar;
add jar hdfs://sandbox.hortonworks.com:8020/esri/spatial-sdk-hive-1.1.1-SNAPSHOT.jar;
add jar hdfs://sandbox.hortonworks.com:8020/esri/spatial-sdk-json-1.1.1-SNAPSHOT.jar;
 
list jars;
 
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
 
