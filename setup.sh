#!/usr/bin/env bash
#
# ProRindeS Setup
#

# import gutils.sh
source gutils.sh; test $? -ne 0 && exit 1

# print usage help message
usage() {
  echo -e "Usage: setup.sh [options] ... \n"
  echo -e "Setup PRinde (ProRindeS) \n"
  echo -e "Options:"
  echo -e " -f, --dest-folder <arg>       \t Installation folder absolute path. Default: /opt/prorindes"
  echo -e " -P, --psims-folder <arg>      \t Installation folder absolute path for pSIMS. Default: /opt/psims"
  echo -e " -D, --dssat-folder <arg>      \t DSSAT folder absolute path. Default: /opt/dssat"
  echo -e " -X, --dssat-executable <arg>  \t DSSAT executable (in DSSAT folder). Default: dscsm047"
  echo -e " -V, --dssat-version <arg>     \t DSSAT version. Default: 47"
  echo -e " -mSB, --sb-model <arg>        \t DSSAT SB model. Default: SBCER047"
  echo -e " -mWH, --wh-model <arg>        \t DSSAT WH model. Default: WHCER047"
  echo -e " -mMZ, --mz-model <arg>        \t DSSAT MZ model. Default: MZCER047"
  echo -e " -mBA, --ba-model <arg>        \t DSSAT BA model. Default: BACER047"
  echo -e " -h, --help                    \t Display a help message and quit."
}

# process script inputs
while [[ $# -gt 0 ]]; do
  case $1 in
    -f|--dest-folder) PRINDE_FOLDER=$2; shift 2;;
    -P|--psims-folder) PSIMS_FOLDER=$2; shift 2;;
    -D|--dssat-folder) DSSAT_FOLDER=$2; shift 2;;
    -X|--dssat-executable) DSSAT_EXECUTABLE=$2; shift 2;;
    -V|--dssat-version) DSSAT_VERSION=$2; shift 2;;
    -mSB|--sb-model) DSSAT_SB_MODEL=$2; shift 2;;
    -mWH|--wh-model) DSSAT_WH_MODEL=$2; shift 2;;
    -mMZ|--mz-model) DSSAT_MZ_MODEL=$2; shift 2;;
    -mBA|--ba-model) DSSAT_BA_MODEL=$2; shift 2;;
    -h|--help|*) usage; exit;;
  esac
done

# set default values when needed
readonly PRINDE_FOLDER=${PRINDE_FOLDER:-/opt/prorindes}
readonly PSIMS_FOLDER=${PSIMS_FOLDER:-/opt/psims}
readonly DSSAT_FOLDER=${DSSAT_FOLDER:-/opt/dssat}
readonly DSSAT_EXECUTABLE=${DSSAT_EXECUTABLE:-dscsm047}
readonly DSSAT_VERSION=${DSSAT_VERSION:-47}
readonly DSSAT_SB_MODEL=${DSSAT_SB_MODEL:-SBCER047}
readonly DSSAT_WH_MODEL=${DSSAT_WH_MODEL:-WHCER047}
readonly DSSAT_MZ_MODEL=${DSSAT_MZ_MODEL:-MZCER047}
readonly DSSAT_BA_MODEL=${DSSAT_BA_MODEL:-BACER047}

# check dependencies
[[ ! -n `which unzip` ]] &&
  report_error "Unzip does not seem to be installed on your system! Please install it to continue (sudo apt install unzip)." &&
  exit 1
[[ ! -d $DSSAT_FOLDER ]] &&
  report_error "DSSAT does not seem to be installed on your system! Please install it to continue (see: https://github.com/danielbonhaure/dssat-installation)." &&
  exit 1
[[ ! -d $PSIMS_FOLDER ]] &&
  report_error "pSIMS does not seem to be installed on your system! Please install it to continue (see: https://github.com/danielbonhaure/psims)." &&
  exit 1

#
#
#

new_script "Starting ProRindeS setup"

sudo apt update


# Set required passwords
new_section "S(1)- Set requires passwords"

read -p 'Password for postgres db user: ' pguser_pass
read -p 'Password for the crc api user: ' crcsas_pass

# Check if passwords aren't blank
if [[ -z ${pguser_pass} || -z ${crcsas_pass} ]]; then
    report_error "ERROR: passwords can't be blank"
    exit 1
fi


# Install Mongo
new_section "S(2)- Install Mongo"

sudo apt install -y mongodb

rinde_exist=$(echo "show collections" | mongo Rinde | grep -w forecasts | wc -l)

if [[ ${rinde_exist} -eq FALSE ]]; then
    echo "db.createCollection('forecasts')" | mongo Rinde
fi


# Install Postgres
new_section "S(3)- Install PostgreSQL"

sudo apt install -y postgresql postgresql-contrib

crcsas_exist=$(sudo -u postgres -H -- psql -l | grep -w crcsas | wc -l)

if [[ ${crcsas_exist} -eq FALSE ]]; then
    sudo -u postgres -H -- psql -c "create database crcsas"
    sudo -u postgres -H -- psql -c "alter user postgres password '${pguser_pass}'"
fi


# Install Python3
new_section "S(4)- Install Python3"

sudo apt install -y python3 python3-dev python3-software-properties
sudo apt install -y build-essential python3-psycopg2

sudo apt install -y libyaml-dev
sudo -H python3 -m pip install PyYAML

sudo -H python3 -m pip install watchdog
sudo -H python3 -m pip install requests
sudo -H python3 -m pip install jsonschema
sudo -H python3 -m pip install pymongo
sudo -H python3 -m pip install apscheduler
sudo -H python3 -m pip install Flask
sudo -H python3 -m pip install gevent
sudo -H python3 -m pip install gevent-websocket
sudo -H python3 -m pip install Flask-SocketIO
sudo -H python3 -m pip install xxhash
sudo -H python3 -m pip install fabric


# Install RScript
new_section "S(5)- Install RScript"

sudo apt install -y r-base
sudo chmod o+w /usr/local/lib/R/site-library  # To be able to install R libraries from a R script
sudo apt install -y libgdal-dev    # Dependency of 'rgdal', a R library
sudo apt install -y libssl-dev     # Dependency of 'lazyeval', a R library
sudo apt install -y libgeos++-dev  # Dependency of 'rgeos', a R library
Rscript --verbose ./core/modules/data_updater/impute_script/Install.R
if [[ $? -ne 0 ]] ; then exit 1; fi


# Setup ProRindeS
new_section "S(6)- Setup ProRindeS"

# Check .tmp/rundir existence (its non-existence causes execution-time errors)
if [[ ! -d .tmp ]]; then
    mkdir .tmp
    mkdir .tmp/rundir
elif [[ ! -d .tmp/rundir ]]; then
    mkdir .tmp/rundir
fi

# Set frontend ip
read -p "FrontEnd ip [10.0.2.80]: " frontend_ip
if [[ -n ${frontend_ip} ]]; then
    sed -i "s/'10.0.2.80'/'${frontend_ip}'/g" ./config/database.yaml
fi

# Set campaign first month
read -p "Campain first month (AR=5, PY=9): " first_month
if [[ -n ${first_month} ]]; then
    sed -i "s/campaign_first_month: 5/campaign_first_month: ${first_month}/g" ./config/system.yaml
fi

# Conf DSSAT and pSIMS
sed -i "s/dscsm0XX/"$DSSAT_EXECUTABLE"/g" ./config/system.yaml
sed -i "s/SBCER0XX/"$DSSAT_SB_MODEL"/g" ./config/system.yaml
sed -i "s/WHCER0XX/"$DSSAT_WH_MODEL"/g" ./config/system.yaml
sed -i "s/MZCER0XX/"$DSSAT_MZ_MODEL"/g" ./config/system.yaml
sed -i "s/BACER0XX/"$DSSAT_BA_MODEL"/g" ./config/system.yaml
sed -i "s|/path/to/psims|"$PSIMS_FOLDER"|g" ./config/system.yaml
sed -i "s|/path/to/dssat|"$DSSAT_FOLDER"|g" ./config/system.yaml
sed -i "s|/path/to/prorindes|"$PRINDE_FOLDER"|g" ./config/system.yaml
# WH Argentina
if [[ ! `grep -w ProRindeS $DSSAT_FOLDER/Genotype/WHCER0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'printf "\n! Added for run ProRindeS\n" >> '$DSSAT_FOLDER'/Genotype/WHCER0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w IN0006 $DSSAT_FOLDER/Genotype/WHCER0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'echo "IN0006 BAG 9                  DFAUL1     5    69   747  28.0  32.5   2.0    95" >> '$DSSAT_FOLDER'/Genotype/WHCER0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w IN0007 $DSSAT_FOLDER/Genotype/WHCER0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'echo "IN0007 Inter-Largo            DFAUL1     5    78   710  25.7  32.4   2.0    95" >> '$DSSAT_FOLDER'/Genotype/WHCER0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w DFAUL1 $DSSAT_FOLDER/Genotype/WHCER0${DSSAT_VERSION}.ECO` ]]; then
  sudo sh -c 'printf "\n! Added for run ProRindeS\n" >> '$DSSAT_FOLDER'/Genotype/WHCER0'$DSSAT_VERSION'.ECO'
  sudo sh -c 'echo "DFAUL1   400   .25   285   190  0.25  0.10   200   0.5   1.7   1.7    15   1.3   5.0  0.10  0.50   400   5.5   6.3   3.5   2.5   1.0   2.5   6.0   4.0   3.0   100   0.0   .85    30   3.0   0.0   -10" >> '$DSSAT_FOLDER'/Genotype/WHCER0'$DSSAT_VERSION'.ECO'
fi
# MZ Argentina
if [[ ! `grep -w UAIC10 $DSSAT_FOLDER/Genotype/MZCER0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'printf "\n! Added for run ProRindeS\n" >> '$DSSAT_FOLDER'/Genotype/MZCER0'$DSSAT_VERSION'.CUL'
  sudo sh -c 'echo "UAIC10 DK 682   120 GSP     . IB0001 245.0 0.000 820.0 950.0  7.50 45.00" >> '$DSSAT_FOLDER'/Genotype/MZCER0'$DSSAT_VERSION'.CUL'
fi
# SB Argentina
if [[ ! `grep -w ProRindeS $DSSAT_FOLDER/Genotype/SBGRO0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'printf "\n! Added for run ProRindeS\n" >> '$DSSAT_FOLDER'/Genotype/SBGRO0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w UA3L03 $DSSAT_FOLDER/Genotype/SBGRO0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'echo "UA3L03 A  3901              . SB0344 13.45 0.320  22.5   7.5  14.5 38.50 28.00 1.000  375. 180.0  1.00  0.19  25.5  2.20  10.0  78.0  .400  .200" >> '$DSSAT_FOLDER'/Genotype/SBGRO0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w UA4L01 $DSSAT_FOLDER/Genotype/SBGRO0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'echo "UA4L01 DM 4800              . SB0401 13.00 0.295  19.0   4.0  14.5 39.00 28.00 1.100  375. 180.0  1.00  0.21  20.0  2.20   8.0  78.0  .400  .200" >> '$DSSAT_FOLDER'/Genotype/SBGRO0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w UA4L03 $DSSAT_FOLDER/Genotype/SBGRO0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'echo "UA4L03 A  4910              . SB0404 13.00 0.305  21.0   4.0  14.5 41.00 28.00 1.000  375. 180.0  1.00  0.19  22.0  2.20  13.0  78.0  .400  .200" >> '$DSSAT_FOLDER'/Genotype/SBGRO0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w UA5M02 $DSSAT_FOLDER/Genotype/SBGRO0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'echo "UA5M02 A  5409              . SB0504 12.70 0.345  22.5   6.0  13.5 35.00 28.00 0.850  325. 180.0  1.00  0.15  23.5  2.25  13.0  78.0  .400  .200" >> '$DSSAT_FOLDER'/Genotype/SBGRO0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w UA6M03 $DSSAT_FOLDER/Genotype/SBGRO0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'echo "UA6M03 A  6445              . SB0602 12.45 0.305  23.5   6.0  12.0 41.50 19.00 0.850  375. 180.0  1.00  0.16  27.5  2.05   9.0  78.0  .400  .200" >> '$DSSAT_FOLDER'/Genotype/SBGRO0'$DSSAT_VERSION'.CUL'
fi
# BA Argentina
if [[ ! `grep -w ProRindeS $DSSAT_FOLDER/Genotype/BACER0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'printf "\n! Added for run ProRindeS\n" >> '$DSSAT_FOLDER'/Genotype/BACER0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w IB0105 $DSSAT_FOLDER/Genotype/BACER0${DSSAT_VERSION}.CUL` ]]; then
  sudo sh -c 'echo "IB0105 ANDREIA            1,2 SY0003     5   100   400    18    45   2.3    89" >> '$DSSAT_FOLDER'/Genotype/BACER0'$DSSAT_VERSION'.CUL'
fi
if [[ ! `grep -w ProRindeS $DSSAT_FOLDER/Genotype/BACER0${DSSAT_VERSION}.ECO` ]]; then
  sudo sh -c 'printf "\n! Added for run ProRindeS\n" >> '$DSSAT_FOLDER'/Genotype/BACER0'$DSSAT_VERSION'.ECO'
fi
if [[ ! `grep -w SY0003 $DSSAT_FOLDER/Genotype/BACER0${DSSAT_VERSION}.ECO` ]]; then
  sudo sh -c 'echo "SY0003   275   .25   175   150   .25   .10   240   0.5   2.6   2.6    13   1.0   2.5  0.30  0.60   400   5.7   6.5   5.0   2.5   1.0   2.6   6.0   7.0   3.0   100   5.0   .85    30   3.0   0.0   -10" >> '$DSSAT_FOLDER'/Genotype/BACER0'$DSSAT_VERSION'.ECO'
fi
sed -i '/@RWUPM RWUMX/!b;n;n;c\ \ 2.02\ \ \ 2.03' 'BACER0'$DSSAT_VERSION'.SPE'
sed -i '/@ NFPU  NFPL  NFGU  NFGL  NFTU  NFTL  NFSU  NFSF/!b;n;n;c\ \ 1.00\ \ 0.00\ \ \ 1.0\ \ \ 0.0\ \ \ 1.0\ \ \ 0.0\ \ \ 0.4\ \ \ 0.1' 'BACER0'$DSSAT_VERSION'.SPE'

# Set passwords
printf "${crcsas_pass}" > ./config/pwd/crcssa_db_admin.pwd
printf "${pguser_pass}" > ./config/pwd/postgres.pwd
printf "${crcsas_pass}" > ./core/modules/data_updater/impute_script/db/PostgreSQL/crcssa_db_admin.pwd
printf "${pguser_pass}" > ./core/modules/data_updater/impute_script/db/PostgreSQL/postgres.pwd


# Restore DB
new_section "S(7)- Restore crcsas DB"
if [[ -f crcsas.zip && ${crcsas_exist} -eq FALSE ]]; then
    unzip crcsas.zip
    export PGPASSWORD="${pguser_pass}"
    pg_restore --host=localhost --username=postgres --dbname=crcsas --no-password --jobs=2 "./crcsas.dump"
    psql --host localhost --username=postgres --dbname=crcsas --no-password --quiet -f "./core/lib/SQL/Base Functions.sql"
    psql --host localhost --username=postgres --dbname=crcsas --no-password --quiet -f "./core/modules/data_updater/impute_script/Schema.sql"
    rm crcsas.dump crcsas.zip
else
    if [[ -f crcsas.zip ]]; then
        rm crcsas.zip
    fi
    if [[ ! -f crcsas.zip && ${crcsas_exist} -eq FALSE ]]; then
        report_warning "WARNING: the database was not restored! Backup not found!!"
    fi
    if [[ ${crcsas_exist} -eq TRUE ]]; then
        report_warning "WARNING: the database already existed and it was not modified!"
    fi
fi

report_finish "S(8)- ProRindeS SETUP finished sussectully"

#
#
#
