#!/bin/bash

echo "INFO: Running Module Deployment Checks " >> $LOG_FILE

if grep -q "ERROR:" "$LOG_FILE" /dev/null|| grep -q "No such file or directory" "$LOG_FILE" /dev/null|| grep -q "Got exception:" "$LOG_FILE" /dev/null|| grep -q "SP2" "$LOG_FILE" /dev/null|| grep -q "no password supplied" "$LOG_FILE" /dev/null; then

 echo "ERROR: The following Module Deployment Errors were encountered :- " >> $LOG_FILE

grep -n "ERROR:" "$LOG_FILE" -B 10 -A 10 > temp
cat temp >> $LOG_FILE
grep -n "Got exception:" "$LOG_FILE" -B 10 -A 10 > temp
cat temp >> $LOG_FILE
grep -n "SP2" "$LOG_FILE" -B 10 -A 10 > temp
cat temp >> $LOG_FILE
grep -n "No such file or directory" "$LOG_FILE" -B 10 -A 10 > temp
cat temp >> $LOG_FILE
grep -n "no password supplied" "$LOG_FILE" -B 8 -A 8 > temp
cat temp >> $LOG_FILE

echo "INFO: Completed Module Deployment Checks" >> $LOG_FILE

exit 1

else

echo "INFO: All Objects compiled successfully" >> $LOG_FILE
echo "INFO: No Problems Found" >> $LOG_FILE
echo "INFO: Completed Module Deployment Checks" >> $LOG_FILE

fi

exit
