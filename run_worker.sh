#!/bin/bash

if [ ! -z $1 ]; then
    if [ "$1" == small ] || [ "$1" == medium ] || [ "$1" == large ] ; then
        echo "hi" $1
    else
        echo ERROR: Option provided is "$1", but it must be one of "[small, medium, large]"
        return
    fi
else
    echo ERROR: No option provided! Must provide an option "[small, medium, large]"
    echo ""
    return
fi

python ec2pgdb_builder.py --role-type worker --worker_dir /home/ubuntu/worker_dir/ --key /home/ubuntu/.ssh/kishori.csail.csv --readyqueue ready_$1
