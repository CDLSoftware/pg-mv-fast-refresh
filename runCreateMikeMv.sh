#! /bin/bash

PackageOwner=mike_pgmview
DataOwner=mike_data
ViewOwner=mike_view
DefaultDatabase=postgres
LoadMinValue=100
LoadMaxValue=258

T1Value=$LoadMinValue
if [ $(($T1Value % 3)) = 0 ]
then
    T1Value=$(($T1Value + 1))
fi

T2MinValue=$LoadMinValue
T2Value=$((($T1Value * $LoadMaxValue) + $T2MinValue))
if [ $(($T2Value % 4)) = 0 ]
then
    T2MinValue=$(($T2MinValue + 1))
    T2Value=$((($T1Value * $LoadMaxValue) + $T2MinValue))
fi

T3Value=$((($T1Value * $LoadMaxValue * $LoadMaxValue) + ($T2MinValue * $LoadMaxValue) + $LoadMinValue))

echo "Enter the PostGre HostName: [localhost]"

read pgHostName

echo
echo "Enter the PostGre Port: [5432]"

read pgPort

echo
echo "Enter the PostGre SuperUser name: [mike]"

read pgSuperUser

echo
echo "Enter the PostGre Database name: [postgres]"

read pgDatabase
echo

echo
echo "Enter the PostGre Materialized View Package Owner: [$PackageOwner]"

read pgPackageOwner
echo

echo
echo "Enter the Owner of the PostGre Source Database Tables: [$DataOwner]"

read pgDataOwner
echo

echo
echo "Enter the Owner of the PostGre Materialized Views: [$ViewOwner]"

read pgViewOwner
echo

if [ -z "$pgHostName" ]
then
    pgHostName=localhost
fi

if [ -z "$pgPort" ]
then
    pgPort=5432
fi

if [ -z "$pgSuperUser" ]
then
    pgSuperUser=mike
fi

if [ -z "$pgDatabase" ]
then
    pgDatabase=postgres
fi

if [ -z "$pgPackageOwner" ]
then
    pgPackageOwner=$PackageOwner
fi

if [ -z "$pgDataOwner" ]
then
    pgDataOwner=$DataOwner
fi

if [ -z "$pgViewOwner" ]
then
    pgViewOwner=$ViewOwner
fi

sed -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$ViewOwner/$pgViewOwner/"         \
    -e "s/$DefaultDatabase/$pgDatabase/"    SaveSet/bulkHarness.sql                 > bulkHarness.sql

sed -e "s/$PackageOwner/$pgPackageOwner/"   \
    -e "s/$ViewOwner/$pgViewOwner/"         SaveSet/createCdlSnapshot.sql           > createCdlSnapshot.sql

sed -e "s/$PackageOwner/$pgPackageOwner/"   \
    -e "s/$PackageOwner/$pgPackageOwner/"   \
    -e "s/$DefaultDatabase/$pgDatabase/"    SaveSet/createMikeSnapshotDD.sql        > createMikeSnapshotDD.sql

sed -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$ViewOwner/$pgViewOwner/"         SaveSet/createMikeTestData.sql          > createMikeTestData.sql

sed -e "s/$PackageOwner/$pgPackageOwner/"   \
    -e "s/$PackageOwner/$pgPackageOwner/"   \
    -e "s/$DefaultDatabase/$pgDatabase/"    \
    -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$ViewOwner/$pgViewOwner/"         \
    -e "s/$ViewOwner/$pgViewOwner/"         SaveSet/createMikeTestDataObjects.sql   > createMikeTestDataObjects.sql

sed -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$ViewOwner/$pgViewOwner/"         \
    -e "s/$DefaultDatabase/$pgDatabase/"    SaveSet/createMViewsForBulkData.sql     > createMViewsForBulkData.sql

sed -e "s/$PackageOwner/$pgPackageOwner/"   \
    -e "s/$DefaultDatabase/$pgDatabase/"    SaveSet/destroyMikeSnapshotDD.sql       > destroyMikeSnapshotDD.sql

sed -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$ViewOwner/$pgViewOwner/"         \
    -e "s/$DefaultDatabase/$pgDatabase/"    SaveSet/destroyMikeTestData.sql         > destroyMikeTestData.sql

sed -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$DefaultDatabase/$pgDatabase/"    SaveSet/generateTestData.sql            > generateTestData.sql

sed -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$DefaultDatabase/$pgDatabase/"    SaveSet/generateTestDataPackages.sql    > generateTestDataPackages.sql

sed -e "s/$PackageOwner/$pgPackageOwner/"   SaveSet/mvApplicationFunctions.sql      > mvApplicationFunctions.sql

sed -e "s/$PackageOwner/$pgPackageOwner/"   SaveSet/mvComplexFunctions.sql          > mvComplexFunctions.sql

sed -e "s/$PackageOwner/$pgPackageOwner/"   SaveSet/mvConstants.sql                 > mvConstants.sql

sed -e "s/$PackageOwner/$pgPackageOwner/"   SaveSet/mvSimpleFunctions.sql           > mvSimpleFunctions.sql

sed -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$ViewOwner/$pgViewOwner/"         \
    -e "s/$PackageOwner/$pgPackageOwner/"   \
    -e "s/$DefaultDatabase/$pgDatabase/"    SaveSet/removeMaterializedViews.sql     > removeMaterializedViews.sql

sed -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$DefaultDatabase/$pgDatabase/"    SaveSet/runGenerateTestData.sh          > runGenerateTestData.sh

sed -e "s/$DataOwner/$pgDataOwner/"         \
    -e "s/$ViewOwner/$pgViewOwner/"         SaveSet/testHarness.sql                 > testHarness.sql

chmod 755 runGenerateTestData.sh

date

echo
echo "Do you want to recreate all test data: [Y/N]: Y"

read pgDestroyTestData
pgDestroyTestData=${pgDestroyTestData:='Y'}
pgDestroyTestData=$(echo $pgDestroyTestData | tr '[a-z]' '[A-Z]')

echo

if [ $pgDestroyTestData == 'Y' ]
then
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser -q -f destroyMikeTestData.sql
fi

echo
echo "Do you want to recreate all the entire environment: [Y/N]: Y"

read pgDestroySnapshotDD
pgDestroySnapshotDD=${pgDestroySnapshotDD:='Y'}
pgDestroySnapshotDD=$(echo $pgDestroySnapshotDD | tr '[a-z]' '[A-Z]')

echo

if [ $pgDestroySnapshotDD == 'Y' ]
then
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser       -q -f destroyMikeSnapshotDD.sql
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser       -q -f createMikeSnapshotDD.sql
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgPackageOwner    -q -f mvConstants.sql
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgPackageOwner    -q -f mvSimpleFunctions.sql
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgPackageOwner    -q -f mvComplexFunctions.sql
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgPackageOwner    -q -f mvApplicationFunctions.sql
fi

echo
echo "Do you want test data: [Y/N]: N"

read pgTestData
pgTestData=${pgTestData:='N'}
pgTestData=$(echo $pgTestData | tr '[a-z]' '[A-Z]')

echo

if [ $pgTestData == 'Y' ]
then
    if [ $pgDestroyTestData == 'Y' ]
    then
        psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser   -q -f createMikeTestDataObjects.sql
    fi

    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner   -q -f generateTestDataPackages.sql
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner   -q -c "TRUNCATE TABLE t3, t2, t1"
    ./runGenerateTestData.sh 1 3
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner   -q -f createMikeTestData.sql
fi

echo
echo "Do you want to run the Test Harness: [Y/N]: N"

read pgTestHarness
pgTestHarness=${pgTestHarness:='N'}
pgTestHarness=$(echo $pgTestHarness | tr '[a-z]' '[A-Z]')

echo

if [ $pgTestHarness == 'Y' ]
then
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner   -q -f testHarness.sql
fi

echo
echo "Do you want bulk test data: [Y/N]: N"

read pgBulkData
pgBulkData=${pgBulkData:='N'}
pgBulkData=$(echo $pgBulkData | tr '[a-z]' '[A-Z]')

echo

if [ $pgBulkData == 'Y' ]
then
    if [ $pgDestroyTestData == 'Y' ] || [ $pgTestData != 'Y' ]
    then
        psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser -q -f destroyMikeTestData.sql
        psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser -q -f createMikeTestDataObjects.sql
    fi

    if [ $pgDestroyTestData == 'Y' ] || [ $pgTestHarness != 'Y' ]
    then
        psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner -q -f CreateMViewsForBulkData.sql
    fi

    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner -q -f generateTestDataPackages.sql
    ./runGenerateTestData.sh $LoadMinValue $LoadMaxValue
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner -q -f bulkHarness.sql -v v1=$T1Value -v v2=$T2Value -v v3=$T3Value
fi

echo
echo "Do you want to create CDL Materialized Views: [Y/N]: N"

read pgCdlViews
pgCdlViews=${pgCdlViews:='N'}
pgCdlViews=$(echo $pgCdlViews | tr '[a-z]' '[A-Z]')

echo

if [ $pgCdlViews == 'Y' ]
then
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner -q -f createCdlSnapshot.sql
fi

echo
echo "Do you want to create Test Materialized View Removal: [Y/N]: N"

read pgRemoveViews
pgRemoveViews=${pgRemoveViews:='N'}
pgRemoveViews=$(echo $pgRemoveViews | tr '[a-z]' '[A-Z]')

echo

if [ $pgRemoveViews == 'Y' ]
then
    psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner -q -f removeMaterializedViews.sql
fi

rm bulkHarness.sql
rm createCdlSnapshot.sql
rm createMikeSnapshotDD.sql
rm createMikeTestData.sql
rm createMikeTestDataObjects.sql
rm createMViewsForBulkData.sql
rm destroyMikeSnapshotDD.sql
rm destroyMikeTestData.sql
rm generateTestData.sql
rm generateTestDataPackages.sql
rm mvApplicationFunctions.sql
rm mvComplexFunctions.sql
rm mvConstants.sql
rm mvSimpleFunctions.sql
rm removeMaterializedViews.sql
rm runGenerateTestData.sh
rm testHarness.sql
