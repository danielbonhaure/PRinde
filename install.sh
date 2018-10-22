#!/usr/bin/env bash
#
# Download this file and execute it!
# wget https://raw.githubusercontent.com/danielbonhaure/PRinde/master/install.sh --output-document=install-prinde.sh
#

if [ ! -d /opt/psims ]; then
    echo ERROR: psims has not yet been installed!! See: https://github.com/danielbonhaure/psims
    exit 1
fi

clear; echo "Instalando ProRindeS"

if [ -d /opt/prorindes ]; then
    sudo rm -rf /opt/prorindes
fi

sudo mkdir /opt/prorindes
sudo chown "$USER":"$USER" /opt/prorindes
sudo chmod g+w /opt/prorindes

git clone https://github.com/danielbonhaure/PRinde.git /opt/prorindes

if [ -f crcsas.zip ]
then
    cp crcsas.zip /opt/prorindes/crcsas.zip
else
    echo WARNING: databse backup not found!!
fi

cd /opt/prorindes
bash -x setup.sh || exit 1

clear; echo "Instalando paquetes R"
cd /opt/prorindes/core/modules/data_updater/impute_script
Rscript --verbose Install.R || exit 1
