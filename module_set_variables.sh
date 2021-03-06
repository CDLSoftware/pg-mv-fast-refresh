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
# INSTALL_TYPE=<INSTALL_TYPE>							The install type for example FULL (New install) - UPDATE (Update existing install). Default value set to FULL
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

export MODULEOWNER=<MODULEOWNER>
export MODULE_HOME=<MODULE_HOME>
export MODULEOWNERPASS=<MODULEOWNERPASS>
export INSTALL_TYPE=FULL
export HOSTNAME=<HOSTNAME>
export PORT=<PORT>
export DBNAME=<DBNAME>
export PGUSERNAME=<PGUSERNAME>
export PGPASSWORD=<PGPASSWORD>
export SOURCEUSERNAME=<SOURCEUSERNAME>
export SOURCEPASSWORD=<SOURCEPASSWORD>
export MVUSERNAME=<MVUSERNAME>
export MVPASSWORD=<MVPASSWORD>
export LOG_FILE=<LOG_PATH>/fast_refresh_module_install_`date +%Y%m%d-%H%M`.log
