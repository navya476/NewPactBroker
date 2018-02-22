#!/bin/bash

export SEDBIN=/bin/sed
export DRP_CF_HTTP_PORT=$(echo $DRP_CF_HTTP_PORT)
#export PACT_BROKER_DATABASE_USERNAME=$(echo $DRP_EV_POSTGRESQL_URL | cut -d: -f2 | cut -d/ -f3)
#export PACT_BROKER_DATABASE_PASSWORD=$(echo $DRP_EV_POSTGRESQL_URL | cut -d@ -f1 | cut -d: -f3)
#export PACT_BROKER_DATABASE_HOST=$(echo $DRP_EV_POSTGRESQL_URL | cut -d@ -f2 | cut -d: -f1)
#export PACT_BROKER_DATABASE_NAME=$(echo $DRP_EV_POSTGRESQL_URL | cut -d/ -f4)
$SEDBIN -i "s/<broker_port>/${DRP_CF_HTTP_PORT}/" /etc/nginx/sites-enabled/webapp.conf 
/sbin/my_init
