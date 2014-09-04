use default;

drop table if exists STG_STREAMING_TARGET;

create table if not exists STG_STREAMING_TARGET (
        SNAME      string
       ,LDATE     string
       ,CDATA     string
) 
partitioned by (PROCESSING_STAGE string)
row format delimited
fields terminated by '\001' lines terminated by '\n'
stored as TextFile
location '/user/hive/warehouse/stg_streaming_target'
;

alter table STG_STREAMING_TARGET add if not exists 
  partition (PROCESSING_STAGE='WORKING') 
  partition (PROCESSING_STAGE='PROCESSED');
