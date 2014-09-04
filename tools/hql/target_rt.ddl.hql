use default;

drop table if exists TARGET_RT;

create table if not exists TARGET_RT (
        SNAME     string
       ,LDATE     string
       ,CDATA     string
) 
partitioned by (PROCESSING_STAGE string)
row format delimited
fields terminated by '\001' lines terminated by '\n'
stored as TextFile
location '/user/hive/warehouse/target_rt'
;

alter table TARGET_RT add if not exists 
  partition (PROCESSING_STAGE='RT') 
  partition (PROCESSING_STAGE='STG')
;

