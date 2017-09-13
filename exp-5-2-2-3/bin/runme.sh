#!/bin/bash
export RADICAL_ENTK_VERBOSE=INFO
export RADICAL_ENTK_PROFILE=True
export RADICAL_PILOT_PROFILE=True
export RADICAL_PILOT_DBURL=mongodb://user:user@ds135444.mlab.com:35444/exp-5-2-2-3
export RP_ENABLE_OLD_DEFINES=True

for i in `seq 2 1 5`; do
    mkdir stampede-trial-$i
    python poe.py xsede.stampede_ssh
    radicalpilot-fetch-json rp.session.*
    mv *.prof rp.session.* stampede-trial-$i
done
