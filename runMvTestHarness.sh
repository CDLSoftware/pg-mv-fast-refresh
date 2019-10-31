DefaultPackageOwner=cdl_pgmview
DataOwner=cdl_data
ViewOwner=cdl_view
DataPassword=aws-oracle

DefaultSuperUser=mike
DefaultHostname=localhost
DefaultDatabase=postgres
DefaultPort=5432

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
echo "Enter the PostGre Materialized View Package Owner: [$DefaultPackageOwner]"

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
    pgHostName=$DefaultHostname
fi

if [ -z "$pgPort" ]
then
    pgPort=$DefaultPort
fi

if [ -z "$pgSuperUser" ]
then
    pgSuperUser=$DefaultSuperUser
fi

if [ -z "$pgDatabase" ]
then
    pgDatabase=$DefaultDatabase
fi

if [ -z "$pgPackageOwner" ]
then
    pgPackageOwner=$DefaultPackageOwner
fi

if [ -z "$pgDataOwner" ]
then
    pgDataOwner=$DataOwner
fi

if [ -z "$pgViewOwner" ]
then
    pgViewOwner=$ViewOwner
fi

psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser       -q  -f TestHarness/destroyMikeTestData.sql          \
                                                                        -v v1=$pgDataOwner                              \
                                                                        -v v2=$pgViewOwner                              2>> /dev/null

psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser       -q  -f TestHarness/createMikeTestDataObjects.sql    \
                                                                        -v v1=$pgDataOwner                              \
                                                                        -v v2=$pgViewOwner                              \
                                                                        -v v3=$pgPackageOwner                           \
                                                                        -v v4=$pgSuperUser                              \
                                                                        -v v5=$pgDatabase

psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner       -q  -f TestHarness/generateTestDataPackages.sql

./TestHarness/runGenerateTestData.sh $pgHostName $pgPort $pgDatabase $pgDataOwner 1 3 3

psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner       -q  -f TestHarness/createMikeTestData.sql

psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgDataOwner       -q  -f TestHarness/testHarness.sql                  \
                                                                        -v v1=ViewOwner
