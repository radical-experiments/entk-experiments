#!/bin/bash
export RADICAL_ENTK_VERBOSE=INFO
export RADICAL_ENTK_PROFILE=True
export RADICAL_PILOT_PROFILE=True
export RADICAL_PILOT_DBURL=mongodb://user:user@ds141464.mlab.com:41464/entk-weak
export RP_ENABLE_OLD_DEFINES=True


tasks_list="128 64 32 16"

mkdir data_strong
for t in $(seq 1 1 1); do
    for tasks in $tasks_list; do
        mkdir data_strong/tasks-${tasks}-trial-$t
        python poe.py $tasks xsede.supermic
        radicalpilot-fetch-json rp.session.*
        mv *.prof rp.session.* data_strong/tasks-${tasks}-trial-$t
    done
done
