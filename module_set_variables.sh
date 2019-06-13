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
# LOG_FILE=<LOG_PATH>              						Path to logfile output location
########################################################################################################################################################################

# Set module deployment variables

export MODULEOWNER=<MODULEOWNER>
export MODULE_HOME=<MODULE_HOME>
export MODULEOWNERPASS=<MODULEOWNERPASS>
export HOSTNAME=<HOSTNAME>
export PORT=<PORT>
export DBNAME=<DBNAME>
export PGUSERNAME=<PGUSERNAME>
export PGPASSWORD=<PGPASSWORD>
export LOG_FILE=<LOG_PATH>/fast_refresh_module_install_`date +%Y%m%d-%H%M`.log
