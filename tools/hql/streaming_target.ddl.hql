use default;

drop table if exists STREAMING_TARGET;

create table if not exists STREAMING_TARGET (
       DATA_FIELD string
) 
partitioned by (LOCAL_DATE string, SITE_NAME string)
row format delimited
fields terminated by '\001' lines terminated by '\n'
stored as TextFile
location '/user/hive/warehouse/streaming_target'
;
