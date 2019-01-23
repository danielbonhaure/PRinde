#!/usr/bin/env bash

readonly TRUE=1
readonly FALSE=0

readonly red=$(tput setaf 1)
readonly green=$(tput setaf 2)
readonly yellow=$(tput setaf 3)
readonly blue=$(tput setaf 4)
readonly reset=$(tput sgr0)

new_script () {
    if [[ $# -eq 0 ]]; then
        printf "\n\n\n\n\n\n\n"
    else
        local message=$1
        echo ${blue}${message}${reset}
    fi
}

new_section () {
    if [[ $# -eq 0 ]]; then
        printf "\n\n\n\n"
    else
        local message=$1
        echo ${green}${message}${reset}
    fi
}

report_warning () {
    if [[ $# -eq 0 ]]; then
        echo ${yellow}"WARNING: see previous lines!!"${reset}
    else
        local message=$1
        echo ${yellow}${message}${reset}
    fi
}

report_error () {
    if [[ $# -eq 0 ]]; then
        echo ${red}"ERROR: see previous lines!!"${reset}
    else
        local message=$1
        echo ${red}${message}${reset}
    fi
}

trap report_error ERR
