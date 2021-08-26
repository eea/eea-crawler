#!/bin/sh
set -e

chown 50000:50000 /custom_config
if [[ "${DEV_ENV:-false}" == "true" ]] ; then
    cd /custom_config


    if [ -d ".git" ]; then
       git pull
    else
       rm -r *
       git clone $github_repo . --depth=5
    fi 
    #on commit ignore the permission changes
    git config core.filemode false

    chmod 777 /custom_config
    chmod -R 775 /custom_config/*
fi
