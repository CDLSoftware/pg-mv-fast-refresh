#! /bin/bash
#-----------------------------------------------------------------------------------------------------------------------------------
# Routine Name: testData.sql
# Author:       Mike Revitt
# Date:         12/11/2018
#-----------------------------------------------------------------------------------------------------------------------------------
# Revision History    Push Down List
#-----------------------------------------------------------------------------------------------------------------------------------
# Date        | Name          | Description
#-------------+---------------+-----------------------------------------------------------------------------------------------------
#             |               |
# 12/11/2018  | M Revitt      | Initial version
# ------------+---------------+-----------------------------------------------------------------------------------------------------
# Background:   PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
#               provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.
#
# Description:  This script creates the SCHEMA and USER to hold the Materialized View Fast Refresh code along with the necessary
#               data dictionary views
#
# Issues:       There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
#               versions 10.5 and 10.3
#
#               https://forums.aws.amazon.com/thread.jspa?messageID=860564
#
# Help:         Help can be invoked by running the rollowing command from within PostGre
#
#               DO $$ BEGIN RAISE NOTICE '%', mv$stringConstants('HELP_TEXT'); END $$;
#
#***********************************************************************************************************************************
# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#***********************************************************************************************************************************

# runGenerateTestData.sh 100 250

SECONDS=0;
pgHostname=$1
pgPort=$2
pgDatabase=$3
pgDataOwner=$4

LoadMinValue=$5
LoadMaxValue=$6
NoOfTables=$7

TotalRows=$((LoadMaxValue - LoadMinValue + 1))
NoParentOfRows=$TotalRows
NoChildOfRows=$((TotalRows * TotalRows))

RowCount=`echo " $NoParentOfRows * 2/3" | bc -l`
TwoThirdsParentRows=`printf "%.0f\n" $RowCount`

RowCount=`echo " $NoParentOfRows * 3/4" | bc -l`
ThreeQuartersParentRows=`printf "%.0f\n" $RowCount`

NoGrandChildOfRows=$((ThreeQuartersParentRows * TwoThirdsParentRows * TotalRows))
TotalRows=$((NoParentOfRows + NoChildOfRows + NoGrandChildOfRows))

echo "Create some volume data"
echo

for i in $( seq $1 $2 )
do
    psql -h $pgHostname -p $pgPort -d $pgDatabase -U $pgDataOwner -q -t -f generateTestData.sql -v v1=$i -v v2=$1 -v v3=$2 -v v4=$3
done

MINUTES=$((SECONDS / 60))
SECONDS=$((SECONDS % 60))

LC_NUMERIC=en_US printf "%'.0f rows of data created in %02d:%02d\n" $TotalRows $MINUTES $SECONDS
echo


