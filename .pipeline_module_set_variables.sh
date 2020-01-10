#!/bin/bash

#######################################################################################################################################################################
# Description: Module Set Variables Script for Fast Refresh installation script
#			   This needs editing before you run the main script.
#
# Script Name: module_set_variables.sh
#
# Execute Example: $ . ./module_set_variables.sh
#
# Shell Variable Name									Description
#
# MODULEOWNER=<MODULEOWNER> 							The module owner username
# MODULE_HOME=<MODULE_HOME>								The Module home path
#	                             						for example /u01/app/bi-feeds
# MODULEOWNERPASS=<MODULEOWNERPASS>						Password for module owner PGRS_MVIEW
# HOSTNAME=<HOSTNAME>									Hostname for database
# PORT=<PORT>											Port for database
# DBNAME=<DBNAME>										Database Name
# PGUSERNAME=<PGUSERNAME>								DB username for the module installation run
# PGPASSWORD=<PGPASSWORD>								DB username password for the module installation run
# SOURCEUSERNAME=<SOURCEUSERNAME>                       DB username for the source tables for the MV
# SOURCEASSWORD=<SOURCEASSWORD>                         DB password for the source tables user
# MVUSERNAME=<MVUSERNAME>                               DB username for the MV owner
# MVPASSWORD=<MVPASSWORD>                               DB password for the MV owner
# LOG_FILE=<LOG_PATH>              						Path to logfile output location
########################################################################################################################################################################

# Set module deployment variables

export MODULEOWNER=testpoc
export MODULE_HOME=/builds/cheshire-datasystems/dba-team/pg-mv-fast-refresh-githubrunner
export MODULEOWNERPASS=password1234
export HOSTNAME=localhost
export PORT=5432
export DBNAME=postgres
export PGUSERNAME=postgres
export PGPASSWORD=password1234
export SOURCEUSERNAME=testpocdata
export SOURCEPASSWORD=password1234
export MVUSERNAME=testpocview
export MVPASSWORD=password1234
export LOG_FILE=/tmp/fast_refresh_module_install_`date +%Y%m%d-%H%M`.log
