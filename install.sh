#!/usr/bin/env bash
#
# Download this file and execute it!
# wget https://raw.githubusercontent.com/danielbonhaure/PRinde/master/install.sh --output-document=install-prinde
#

# import gutils.sh
gutils=$(dirname $(readlink -f $0))/gutils.sh
wget --quiet https://raw.githubusercontent.com/danielbonhaure/PRinde/master/gutils.sh --output-document=${gutils}
source ${gutils}; test $? -ne 0 && exit 1

# print usage help message
usage() {
  echo -e "Usage: install-prinde [options] ... \n"
  echo -e "Clone, configure and install PRinde (ProRindeS) \n"
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
    -mSB|--sb-model) DSSAT_SB_MODEL=$2; shift 2;;
    -mWH|--wh-model) DSSAT_WH_MODEL=$2; shift 2;;
    -mMZ|--mz-model) DSSAT_MZ_MODEL=$2; shift 2;;
    -mBA|--ba-model) DSSAT_BA_MODEL=$2; shift 2;;
    -V|--dssat-version) DSSAT_VERSION=$2; shift 2;;
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
[[ ! -n `which git` ]] &&
  report_error "Git does not seem to be installed on your system! Please install it to continue (sudo apt install git)." &&
  exit 1
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

new_script "Install ProRindeS"

report_warning "Caution!!"
echo "This script must be executed by the user who will run prorindes (note that this user must be capable to initiate a password-less SSH session in the frontend!)"
report_warning "Do you want to continue?"
select yn in "Yes" "No"; do
   case ${yn} in
        Yes ) break;;
        No ) exit;;
    esac
done

new_section "I(1)- Check if pSIMS has already been installed!"

if [[ ! -d $PSIMS_FOLDER ]]; then
    report_error "ERROR: pSIMS has not yet been installed!! See: https://github.com/danielbonhaure/psims"
    exit 1
fi


new_section "I(2)- Remove old instalation (if exist)"

if [[ -e /lib/systemd/system/prorindes.service ]]; then
    sudo systemctl stop prorindes.service
    sudo systemctl disable prorindes.service
    sudo rm /lib/systemd/system/prorindes.service
    sudo systemctl daemon-reload
fi

if [[ -d $PRINDE_FOLDER ]]; then
    sudo rm -rf $PRINDE_FOLDER; test $? -ne 0 && exit 1
fi

sudo mkdir $PRINDE_FOLDER
sudo chown "${USER}":"${USER}" $PRINDE_FOLDER
sudo chmod g+w $PRINDE_FOLDER


new_section "I(3)- Clone ProRindeS github repository"
git clone https://github.com/danielbonhaure/PRinde.git $PRINDE_FOLDER
if [[ $? -ne 0 ]] ; then exit 1; fi

if [[ -f crcsas.zip ]]
then
    cp crcsas.zip $PRINDE_FOLDER/crcsas.zip
else
    report_warning "WARNING: database backup not found!!"
fi

rm gutils.sh
cd $PRINDE_FOLDER
bash setup.sh -f $PRINDE_FOLDER -P $PSIMS_FOLDER -D $DSSAT_FOLDER -X $DSSAT_EXECUTABLE -V $DSSAT_VERSION -mSB $DSSAT_SB_MODEL -mWH $DSSAT_WH_MODEL -mMZ $DSSAT_MZ_MODEL -mBA $DSSAT_BA_MODEL
if [[ $? -ne 0 ]] ; then exit 1; fi

new_section "I(4)- Conf ProRindeS to run at startup"

cat <<EOF | sudo tee /lib/systemd/system/prorindes.service >/dev/null
# ver:
# 1- https://www.digitalocean.com/community/tutorials/understanding-systemd-units-and-unit-files
# 2- https://fedoramagazine.org/systemd-unit-dependencies-and-order/

[Unit]
Description=ProRindeS Service
After=multi-user.target
After=network-online.target
Requires=network-online.target

[Service]
User=${USER}
Type=simple
ExecStart=/usr/bin/python3 ${PRINDE_FOLDER}/main.py

[Install]
WantedBy=multi-user.target

EOF

sudo systemctl daemon-reload
sudo systemctl enable prorindes.service
sudo systemctl start prorindes.service

report_finish "I(5)- ProRindeS INSTALLATION finished sussectully"
