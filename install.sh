#!/usr/bin/env bash

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

cd /opt/prorindes
bash setup.sh
