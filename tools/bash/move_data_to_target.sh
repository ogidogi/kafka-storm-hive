#!/bin/bash

#----------------------------------------------------------------------
# Notes:
#     1. Used INSERT INTO stg table
#     2. STG and TARGET table should have same PARTITION names and structure
#     3. Automatic Unlock tables when exit through CLEANUP function and trap rule
#     4. Files in Real-Time should be with *.txt extension
#
#----------------------------------------------------------------------

set -o errexit
set -o pipefail
set -o nounset
umask 0002

SCRIPT_PATH=${0%/*}
SCRIPT_NAME=${0##*/}

DEFAULT_DB='DEFAULT'
RT_TABLE='TARGET_RT'
STG_TABLE='TARGET_STG'
TARGET_TABLE='TARGET'

PROCESSED_FILES=''

# FIRST_PART='local_date'
# SECOND_PART='site_name'

LOG() {
   echo "[$(date +'%y-%m-%d %H:%M:%S') ${0##*/}] $*"
}

RELEASE_LOCK() {
   #----------------------------------------------------------------------
   # Release locks from Real-Time Table
   #----------------------------------------------------------------------
   set +o errexit
   hive -S -e "use $DEFAULT_DB; unlock table $RT_TABLE"
   LOG "INFO: $RT_TABLE unlocked successfully"

   hive -S -e "use $DEFAULT_DB; unlock table $TARGET_TABLE"
   LOG "INFO: $TARGET_TABLE unlocked successfully"
   set -o errexit
}

CLEANUP() {
   #----------------------------------------------------------------------
   # Clean up procedure before exit
   #----------------------------------------------------------------------
   LOG "CLEANUP: Release Hive LOCKs"
   RELEASE_LOCK
   LOG "-== $SCRIPT_NAME FINISHED ==-"
}

trap CLEANUP EXIT

# Debug
# RELEASE_LOCK


#----------------------------------------------------------------------
# Populate STG table
#----------------------------------------------------------------------
LOG "-== START $SCRIPT_NAME ==-"

RT_TABLE_LOCATION=$(hive -S -e "use $DEFAULT_DB; describe formatted $RT_TABLE;" | grep 'Location' | awk '{ print $NF }')
LOG "INFO: Obtain location of $RT_TABLE: $RT_TABLE_LOCATION"

STG_TABLE_LOCATION=$(hive -S -e "use $DEFAULT_DB; describe formatted $STG_TABLE;" | grep 'Location' | awk '{ print $NF }')
LOG "INFO: Obtain location of $STG_TABLE: $STG_TABLE_LOCATION"

TARGET_TABLE_LOCATION=$(hive -S -e "use $DEFAULT_DB; describe formatted $TARGET_TABLE;" | grep 'Location' | awk '{ print $NF }')
LOG "INFO: Obtain location of $TARGET_TABLE: $TARGET_TABLE_LOCATION"

LOG "INFO: Populate $STG_TABLE table" 

FILES_WITH_MD5=$(hadoop fs -ls $RT_TABLE_LOCATION | grep '.chksum' | awk '{ print $NF }')
if [ -z "$FILES_WITH_MD5" ]; then
   LOG "INFO: No files with MD5 cheksums under $RT_TABLE_LOCATION"
   exit 0
fi

# Populate file list with quotas, comas
FILES_WITH_DATA=$(echo $FILES_WITH_MD5 | sed 's/.chksum/.txt/g')
HIVE_FILES_FILTER=$(echo $FILES_WITH_DATA | sed "s/ /','/g" | sed "s/^/'/" | sed "s/$/'/")
LOG "INFO: List of files with cheksums:" $HIVE_FILES_FILTER

set +o errexit
# -hiveconf HIVE_FILES_FILTER=${HIVE_FILES_FILTER}
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
from $RT_TABLE
where  INPUT__FILE__NAME in ($HIVE_FILES_FILTER)
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
FILE_LIST=$(hadoop fs -ls -R $STG_TABLE_LOCATION | grep -v "^d" | awk '{ print $NF }')
LOG "INFO: Obtain file names of $STG_TABLE:"
echo "$FILE_LIST"


# Lock Business and Real-Time Tables
LOG "INFO: Lock $RT_TABLE table"
hive -S -e "use $DEFAULT_DB; lock table $RT_TABLE exclusive"

LOG "INFO: Lock $TARGET_TABLE table"
hive -S -e "use $DEFAULT_DB; lock table $TARGET_TABLE exclusive"


# Start move files
ITER_COUNT=$(echo "$FILE_LIST" | wc -w)
ITER_NUM=0
SUCCESSFULL_PARTITION=''

for CUR_FILE in $FILE_LIST
do
   LOG "----------------------------------------------------------------------"
   ITER_NUM=$(expr $ITER_NUM + 1)
   LOG "INFO: Iteration ${ITER_NUM}/${ITER_COUNT} ..."
   LOG "INFO: Processing $CUR_FILE"
   # Obtain: part1=X/../partN=X/file_name
   PARTITION_PATH=$(echo $CUR_FILE | sed "s#$STG_TABLE_LOCATION/##")
   # echo PARTITION_PATH = $PARTITION_PATH

   # Obtain: part1=X/../partN=X
   PARTITION_PATH_ONLY=${PARTITION_PATH%/*}
   # echo PARTITION_PATH_ONLY = $PARTITION_PATH_ONLY
   
   # Obtain: file_name
   FILE_NAME_ONLY=${PARTITION_PATH##*/}
   # echo FILE_NAME_ONLY = $FILE_NAME_ONLY
   
   TS=$(date +%s)
   TARGET_DEST=${TARGET_TABLE_LOCATION}/${PARTITION_PATH_ONLY}/${TS}_target_stg
   # echo TARGET_DEST = $TARGET_DEST

   set +o errexit
   LOG "INFO: Create location (${TARGET_TABLE_LOCATION}/${PARTITION_PATH_ONLY}) in $TARGET_TABLE"
   hadoop fs -mkdir -p ${TARGET_TABLE_LOCATION}/${PARTITION_PATH_ONLY}
   hadoop fs -mv $CUR_FILE $TARGET_DEST
   if [ $? -ne 0 ] ; then
      LOG "ERROR: Moving $CUR_FILE to $TARGET_DEST Failed!"
      # exit 3
   else
      LOG "INFO: Moving $CUR_FILE to $TARGET_DEST Succeeded"
      # For CSV
      # if [ "x$PROCESSED_FILES" = "x" ]; then PROCESSED_FILES=${CUR_FILE}; else PROCESSED_FILES="${PROCESSED_FILES};${CUR_FILE}"; fi
      # PROCESSED_FILES="${PROCESSED_FILES} ${CUR_FILE}"
      
      # Format: part1='X',..,partN='X'
      _PARTITION_SPEC=$(echo $PARTITION_PATH_ONLY | sed -e "s/=/='/g" -e "s#/#',#g" -e "s/$/'/")
      SUCCESSFULL_PARTITION="${SUCCESSFULL_PARTITION} partition ($_PARTITION_SPEC)"
   fi
   set -o errexit
done
LOG "----------------------------------------------------------------------"


#----------------------------------------------------------------------
# Add partition in Business Table (METADATA)
#----------------------------------------------------------------------
hive -S -e "use $DEFAULT_DB; unlock table $TARGET_TABLE"
LOG "INFO: List of partition for add in $TARGET_TABLE: $SUCCESSFULL_PARTITION"
set +o errexit
echo time hive -S -e "use $DEFAULT_DB; alter table $TARGET_TABLE add if not exists $SUCCESSFULL_PARTITION"
time hive -S -e "use $DEFAULT_DB; alter table $TARGET_TABLE add if not exists $SUCCESSFULL_PARTITION"

if [ $? -ne 0 ] ; then
   LOG "ERROR: Adding partition into $TARGET_TABLE Failed!"
   # exit 3
else
   LOG "INFO: Adding partition into $TARGET_TABLE Succeeded"
   LOG "INFO: Following partitions has been added: $SUCCESSFULL_PARTITION"
fi
set -o errexit


#----------------------------------------------------------------------
# Delete data from STG Table
#----------------------------------------------------------------------
# LOG "INFO: Delete files from $STG_TABLE"
# echo PROCESSED_FILES = $PROCESSED_FILES
# for FILE_FOR_DELETE in $PROCESSED_FILES
# do
   # hadoop fs -rm -r -f $FILE_FOR_DELETE
   # LOG "INFO: $FILE_FOR_DELETE deleted"
# done


#----------------------------------------------------------------------
# Delete data from Real-Time Table
#----------------------------------------------------------------------
FILES_FOR_DELETE="${FILES_WITH_DATA} ${FILES_WITH_MD5}"
LOG "INFO: Delete files from $RT_TABLE: $FILES_FOR_DELETE"

for i in $FILES_FOR_DELETE
do
   hadoop fs -rm -r -f $i
   LOG "INFO: $i deleted"
done

exit 0

