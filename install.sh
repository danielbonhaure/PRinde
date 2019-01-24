#!/usr/bin/env bash
#
# Download this file and execute it!
# wget https://raw.githubusercontent.com/danielbonhaure/PRinde/master/install.sh --output-document=install-prinde.sh
#

sudo echo ""; if [[ $? -ne 0 ]] ; then exit 1; fi
wget --quiet https://raw.githubusercontent.com/danielbonhaure/PRinde/master/gutils.sh --output-document=gutils.sh
source gutils.sh; if [[ $? -ne 0 ]] ; then exit 1; fi

#
#
#

new_script "Install ProRindeS"


new_section "1- Check if pSIMS has already been installed!"

if [[ ! -d /opt/psims ]]; then
    report_error "ERROR: pSIMS has not yet been installed!! See: https://github.com/danielbonhaure/psims"
    exit 1
fi


new_section "2- Remove old instalation (if exist)"

if [[ -d /opt/prorindes ]]; then
    sudo rm -rf /opt/prorindes
    if [[ $? -ne 0 ]] ; then exit 1; fi
fi

sudo mkdir /opt/prorindes
sudo chown "${USER}":"${USER}" /opt/prorindes
sudo chmod g+w /opt/prorindes


new_section "3- Clone ProRindeS github repository"
git clone https://github.com/danielbonhaure/PRinde.git /opt/prorindes
if [[ $? -ne 0 ]] ; then exit 1; fi

if [[ -f crcsas.zip ]]
then
    cp crcsas.zip /opt/prorindes/crcsas.zip
else
    report_warning "WARNING: database backup not found!!"
fi

rm gutils.sh
cd /opt/prorindes
bash setup.sh
