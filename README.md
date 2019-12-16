# Postgres materialized View Fast Refresh module

This project enables Postgres fast refresh capability using materialised view logs to track changes and offer an alternative to the complete refresh.

The fast refresh process was designed to be installed into its own schema that contains the functions needed to run the MV process, with three data dictionary tables and 3 roles.  

The workflow for the MV log creation is shown in the diagram below:


The workflow for the MV creation is shown in the diagram below:


## Installing the module

The install of the fast refresh functions is designed to live in its own schema in the database that is specified via the MODULEOWNER parameter.  

To install the MV code, you need to navigate to the folder where the repo has been downloaded and edit the module_set_variables.sh file. This is where all the variables are stored for where we want to install the fast refresh functions.  

The SOURCEUSERNAME/SOURCEPASSWORD & MVUSERNAME/MVPASSWORD parameters are not needed to install the fast refresh functions they are used for the test harness set-up.


``` bash
cd pg-mv-fast-refresh
vi module_set_variables.sh

MODULEOWNER=<MODULEOWNER> - The module owner username
MODULE_HOME=<MODULE_HOME> - The Module home path 
MODULEOWNERPASS=<MODULEOWNERPASS> - Password for module owner PGRS_MVIEW
HOSTNAME=<HOSTNAME> - Hostname for database
PORT=<PORT>	 - port for database
DBNAME=<DBNAME>	 - Database Name
PGUSERNAME=<PGUSERNAME> - DB username for the module installation run
PGPASSWORD=<PGPASSWORD> - DB username password for the module installation run
SOURCEUSERNAME=<SOURCEUSERNAME> - DB username for the source tables for the MV
SOURCEPASSWORD=<SOURCEPASSWORD> - DB password for the source tables user
MVUSERNAME=<MVUSERNAME> - DB username for the MV owner
MVPASSWORD=<MVPASSWORD> - DB password for the MV owner
LOG_FILE=<LOG_PATH> - Path to logfile output location

```

Here is an example of the parameter settings used the test case: we have an RDS instance pg-tonytest.test.com with a database testpoc and a master username dbamin 

The fast refresh functions will be installed under the schema testpoc by the install package. 

We then have a source schema testpocsource. This is where the source data tables will go for the test harness and a testpocmv, which is the schema where the MV will be built.


``` bash
export MODULEOWNER=testpoc
export MODULE_HOME=/var/lib/pgsql/pg-mv-fast-refresh
export MODULEOWNERPASS=xxxxxxx
export HOSTNAME=pg-tonytest.test.com
export PORT=5432
export DBNAME=postgres
export PGUSERNAME=dbadmin
export PGPASSWORD=xxxxxxx
export SOURCEUSERNAME=testpocsource
export SOURCEPASSWORD=xxxxxxx
export MVUSERNAME=testpocmv
export MVPASSWORD=xxxxxxx
export LOG_FILE=/tmp/fast_refresh_module_install_`date +%Y%m%d-%H%M`.log

```

Now change the permissions on the script runCreateFastRefreshModule.sh to execute and then run. 

``` bash
chmod 700 runCreateFastRefreshModule.sh
$ ./runCreateFastRefreshModule.sh
Check log file - /tmp/fast_refresh_module_install_20191119-1423.log
$

```

This should just take seconds to run. When it’s complete, check the log file in the location you set. In my example, it’s in /tmp. The status is shown at the bottom; below is the example of the run I performed.

``` bash
cat /tmp/fast_refresh_module_install_20191119-1423.log
INFO: Set variables
INFO: LOG_FILE parameter set to /tmp/fast_refresh_module_install_20191119-1423.log
INFO: MODULEOWNER parameter set to testpoc
INFO: PGUSERNAME parameter set to dbadmin
INFO: HOSTNAME parameter set to pg-tonytest.test.com
INFO: PORT parameter set to 5432
INFO: DBNAME parameter set to postgres
INFO: MODULE_HOME parameter set to /var/lib/pgsql/pg-mv-fast-refresh
INFO: Run testpoc schema build script
INFO: Connect to postgres database postgres via PSQL session
…….. cut lines………..
GRANT
INFO: Running Module Deployment Error Checks
INFO: All Objects compiled successfully
INFO: No Errors Found
INFO: Completed Module Deployment Error Checks


```

After this install the functions will be installed under the MODULEOWNER schema.

## Removing the module

To uninstall the module just execute the dropFastRefreshModule.sh script and it will prompt you to ask if you want to remove the module schema.
 
``` bash
$ ./dropFastRefreshModule.sh
Are you sure you want to remove the module schema - testpoc (y/n)?y
yes selected the schemas - testpoc will be dropped
INFO: Drop Module Schema complete check logfile for status - /tmp/dropFastRefreshModule_20191119-1430.log

``` 

## Test Harness Install 

There is a test harness script create_test_harness.sh that will create six tables and insert some data into the tables and then create a complex MV.  The script is exceuted as below

The SOURCEUSERNAME/SOURCEPASSWORD & MVUSERNAME/MVPASSWORD parameters are needed in the module_set_variables.sh.  The SOURCEUSERNAME is the schema where the base tables will be created and the MVUSERNAME is the schema where the materialized view will be created.

``` bash
$ pwd
/var/lib/pgsql/pg-mv-fast-refresh/test_harness
$ ./create_test_harness.sh
INFO: Build Complete check logfile for status - /tmp/test_harness_install_20191119-1425.log
```

If you check the output of the log file you will see the objects being created and the MV being created.

## Test Harness Removal

To remove the test harness just execute the drop_test_harness.sh script and this will remove the test objects.

``` bash
$ ./drop_test_harness.sh
INFO: Drop Complete check logfile for status - /tmp/test_harness_drop_20191119-1428.log 

```

## Pipeline Checks

There is a pipeline checks scripts that will install the module, create some test data and build 90 materialized view's then drop all the objects, schemas and users.   This is mandatory to run if you want to contribute to the code it confirms that the modules will deploy ok and the MV's create with no errors.

``` bash
$ ./run_pipeline_checks.sh all
Starting pipeline script with option all
Starting time - Mon Dec 16 12:54:08 UTC 2019
Stage 1: Creating the fast refresh module objects in schemas testpoc
Stage 2: Creating the schemas testpocdata and testpocview
Stage 3: Creating the test objects and data in schema testpocdata
Stage 4: Creating the MV logs in testpocdata
Stage 5: Creating 90 test MV's in schema testpocview
Stage 6: Test phase
Stage 6.1: Update 1 row and refresh all MV's
Stage 7: Dropping the test harness objects
Stage 8: Check for problems
Pipeline run type all ran with no issues
Run completion time - Mon Dec 16 12:55:02 UTC 2019

```