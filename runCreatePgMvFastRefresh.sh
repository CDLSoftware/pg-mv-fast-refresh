DefaultPackageOwner=cdl_pgmview
DefaultSuperUser=mike
DefaultHostname=localhost
DefaultDatabase=postgres
DefaultPort=5432
DataOwner=cdl_data
ViewOwner=cdl_view


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

psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser       -q  -f TestHarness/destroyMikeTestData.sql      \
                                                                        -v v1=$DataOwner                            \
                                                                        -v v2=$ViewOwner                            2>> /dev/null

psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser       -q  -f BuildScripts/destroyMikeSnapshotDD.sql   \
                                                                        -v v1="$pgPackageOwner"

psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser       -q  -f BuildScripts/createMikeSnapshotDD.sql    \
                                                                        -v v1="$pgSuperUser"                        \
                                                                        -v v2="$pgPackageOwner"                     \
                                                                        -v v4="$pgDatabase"

psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgPackageOwner    -q  -f BuildScripts/mvTypes.sql
psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgPackageOwner    -q  -f BuildScripts/mvConstants.sql
psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgPackageOwner    -q  -f BuildScripts/mvSimpleFunctions.sql
psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgPackageOwner    -q  -f BuildScripts/mvComplexFunctions.sql
psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgPackageOwner    -q  -f BuildScripts/mvApplicationFunctions.sql
psql -h $pgHostName -p $pgPort -d $pgDatabase -U $pgSuperUser       -q  -f BuildScripts/secureMikeSnapshotDD.sql    \
                                                                        -v v1="$pgPackageOwner"
