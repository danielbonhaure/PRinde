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


new_section "I(1)- Check if pSIMS has already been installed!"

if [[ ! -d /opt/psims ]]; then
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

if [[ -d /opt/prorindes ]]; then
    sudo rm -rf /opt/prorindes
    if [[ $? -ne 0 ]] ; then exit 1; fi
fi

sudo mkdir /opt/prorindes
sudo chown "${USER}":"${USER}" /opt/prorindes
sudo chmod g+w /opt/prorindes


new_section "I(3)- Clone ProRindeS github repository"
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
Type=simple
ExecStart=/usr/bin/python3 /opt/prorindes/main.py

[Install]
WantedBy=multi-user.target

EOF

sudo systemctl daemon-reload
sudo systemctl enable prorindes.service
sudo systemctl start prorindes.service

report_finish "I(5)- ProRindeS INSTALLATION finished sussectully"