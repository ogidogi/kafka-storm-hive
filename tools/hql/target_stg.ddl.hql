use default;

drop table if exists TARGET_STG;

create table if not exists TARGET_STG (
        DATA_FIELD string
) 
partitioned by (LOCAL_DATE string, SITE_NAME string)
row format delimited
fields terminated by '\001' lines terminated by '\n'
stored as TextFile
location '/user/hive/warehouse/target_stg'
;

