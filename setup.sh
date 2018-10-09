#!/usr/bin/env bash

if [ ! -d /opt/psims ]; then
    echo ERROR: psims has not yet been installed!! See: https://github.com/danielbonhaure/psims
    exit 1
fi

clear; echo "Starting ProRindeS setup"

sudo apt update

# Install Mongo
sudo apt install -y mongodb
echo "db.createCollection('forecasts')" | mongo Rinde

# Install Postgres
sudo apt install -y postgresql postgresql-contrib
sudo -u postgres -H -- psql -c "create database crcsas"
sudo -u postgres -H -- psql -c "alter user postgres password 'prorindes'"

# Setup
sudo apt install -y python3 python3-dev python3-software-properties
sudo apt install -y build-essential python3-pip python3-psycopg2

sudo apt install -y libyaml-dev
sudo -H pip3 install PyYAML

sudo -H pip3 install watchdog
sudo -H pip3 install requests
sudo -H pip3 install jsonschema
sudo -H pip3 install pymongo
sudo -H pip3 install apscheduler
sudo -H pip3 install Flask
sudo -H pip3 install gevent
sudo -H pip3 install Flask-SocketIO
sudo -H pip3 install xxhash

# Install RScript
sudo apt install -y r-base
sudo chmod o+w /usr/local/lib/R/site-library  # To be able to install R libraries from a R script
sudo apt install -y libproj-dev libgdal-dev   # Dependency of 'rgdal', a R library
sudo apt install -y libcurl4-openssl-dev      # Dependency of 'lazyeval', a R library

# Check .tmp/rundir existence (its non-existence causes execution-time errors)
if [ ! -d .tmp ]; then
    mkdir .tmp
    mkdir .tmp/rundir
elif [ ! -d .tmp/rundir ]; then
    mkdir .tmo/rundir
fi

# Restore DB
if [ -f crcsas.zip ]
then
    unzip crcsas.zip
    psql --host localhost --username=postgres --dbname=crcsas -W --quiet -f "./crc.backup.sql"
    psql --host localhost --username=postgres --dbname=crcsas -W --quiet -f "./core/lib/SQL/Base Functions.sql"
    psql --host localhost --username=postgres --dbname=crcsas -W --quiet -f "./core/modules/data_updater/impute_script/Schema.sql"
    rm crc.backup.sql crcsas.zip
else
    echo WARNING: the database was not restored!
fi
