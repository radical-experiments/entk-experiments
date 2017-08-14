#!/bin/bash

export RADICAL_ENTK_VERBOSE=INFO
export RADICAL_ENTK_PROFILE=True
export RADICAL_PILOT_PROFILE=True
export RADICAL_PILOT_DBURL=mongodb://user:user@ds153652.mlab.com:53652/test_08_08
export RP_ENABLE_OLD_DEFINES=True

pipelines="64 128 256"

#mkdir app_sleep
for i in $pipelines; do
    mkdir app_sleep/pipes-$i
    python poe.py $i sleep xsede.supermic
    radicalpilot-fetch-json rp.session.*
    mv *.prof rp.session.* app_sleep/pipes-$i

done
