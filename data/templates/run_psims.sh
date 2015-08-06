#!/usr/bin/env bash

cd "%s" # pSIMS path.
rm -rf "./campaigns/run/*"
rm -rf "./campaigns/run"
cp -rf "%s" "./campaigns/run" # Rundir path.
./psims -s local -p "./campaigns/run/params" -c "./campaigns/run" -g "./campaigns/run/gridList.txt"