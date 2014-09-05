#!/bin/bash

#----------------------------------------------------------------------
# Notes:
#     1. Used INSERT INTO stg table
#     2. STG and TARGET table should have same PARTITION names and structure
#
# TO-DO:
#     1. Move 'Release Locks' into function
#     2. Provide DEBUG_MODE var, diff level of output
#     3. Not Work from moving section
#
#----------------------------------------------------------------------

set -o errexit
set -o pipefail
set -o nounset
umask 0002

LOG() {
   echo "[$(date +'%y-%m-%d %H:%M:%S') ${0##*/}] $*"
}

SCRIPTPATH=${0%/*}
SCRIPTNAME=${0##*/}

DEFAULT_DB='DEFAULT'
REAL_TIME_TABLE='DEFAULT.TARGET_RT'
STG_TABLE='DEFAULT.TARGET_STG'
TARGET_TABLE='DEFAULT.TARGET'

PROCESSED_FILES=''

#FIRST_PART='local_date'
#SECOND_PART='site_name'

# Debug
hive -S -e "use $DEFAULT_DB; unlock table $REAL_TIME_TABLE"
hive -S -e "use $DEFAULT_DB; unlock table $TARGET_TABLE"


#----------------------------------------------------------------------
# Populate STG table
#----------------------------------------------------------------------
LOG "-== STARTED ==-"
LOG "INFO: Populate $STG_TABLE table" 

set +o errexit
time hive -v -e \
"
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

use $DEFAULT_DB;

insert into table $STG_TABLE partition (LOCAL_DATE, SITE_NAME)
select 
    cdata
   ,ldate
   ,sname
from $REAL_TIME_TABLE
where  INPUT__FILE__NAME like '%-processed'
distribute by ldate, sname
;
"

if [ $? -ne 0 ] ; then
   LOG "ERROR: Populating STG table Failed!"
   exit 3
else
   LOG "INFO: Populating STG table Succeeded"
fi
set -o errexit


#----------------------------------------------------------------------
# Move data to Business Table
#----------------------------------------------------------------------
STG_TABLE_LOCATION=$(hive -S -e "describe formatted $STG_TABLE;" | grep 'Location' | awk '{ print $NF }')
LOG "INFO: Obtain location of $STG_TABLE: $STG_TABLE_LOCATION"

TARGET_TABLE_LOCATION=$(hive -S -e "describe formatted $TARGET_TABLE;" | grep 'Location' | awk '{ print $NF }')
LOG "INFO: Obtain location of $TARGET_TABLE: $TARGET_TABLE_LOCATION"

FILE_LIST=$(hadoop fs -ls -R $STG_TABLE_LOCATION | grep -v "^d" | awk '{ print $NF }')
LOG "INFO: Obtain file names of $STG_TABLE:"
echo "$FILE_LIST"


# Lock Business and Real-Time Tables
LOG "INFO: Lock $REAL_TIME_TABLE table"
hive -S -e "use $DEFAULT_DB; lock table $REAL_TIME_TABLE exclusive"

LOG "INFO: Lock $TARGET_TABLE table"
hive -S -e "use $DEFAULT_DB; lock table $TARGET_TABLE exclusive"


# Start move files
for CUR_FILE in $FILE_LIST
do
   LOG "INFO: Processing $CUR_FILE ..."
   PARTITION_PATH=$(echo $CUR_FILE | sed "s#$STG_TABLE_LOCATION##")
   # echo PARTITION_PATH = $PARTITION_PATH
   TARGET_DEST=${TARGET_TABLE_LOCATION}${PARTITION_PATH}
   # echo TARGET_DEST = $TARGET_DEST
   set +o errexit
   PARTITION_ONLY=${PARTITION_PATH##*/}
   echo PARTITION_ONLY = $PARTITION_ONLY
   echo hadoop fs -mkdir -p ${TARGET_TABLE_LOCATION}${PARTITION_ONLY}
   hadoop fs -mv $CUR_FILE $TARGET_DEST
   if [ $? -ne 0 ] ; then
      LOG "ERROR: Moving $CUR_FILE to $TARGET_DEST Failed!"
      # exit 3
   else
      LOG "INFO: Moving $CUR_FILE to $TARGET_DEST Succeeded"
      # For CSV
      #if [ "x$PROCESSED_FILES" = "x" ]; then PROCESSED_FILES=${CUR_FILE}; else PROCESSED_FILES="${PROCESSED_FILES};${CUR_FILE}"; fi
      PROCESSED_FILES="${PROCESSED_FILES} ${CUR_FILE}"
   fi
   set -o errexit
done


#----------------------------------------------------------------------
# Delete data from Real-Time Table
#----------------------------------------------------------------------
for FILE_FOR_DELETE in $PROCESSED_FILES
do
   hadoop fs -rm -r -f $FILE_FOR_DELETE
   LOG "INFO: $FILE_FOR_DELETE deleted"
done


#----------------------------------------------------------------------
# Release locks from Real-Time Table
#----------------------------------------------------------------------

LOG "-== FINISHED ==-"
exit 0