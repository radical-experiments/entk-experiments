#!/bin/bash
export RADICAL_ENTK_VERBOSE=INFO
export RADICAL_ENTK_PROFILE=True
export RADICAL_PILOT_PROFILE=True
export RADICAL_PILOT_DBURL='mongodb://user:user@ds127564.mlab.com:27564/entk-09-08'
export RP_ENABLE_OLD_DEFINES=True

for t in `seq 2 1 5`; do
    mkdir pipe-16-trial-$t
    python pipeline_16_stage_1_task_1.py
    radicalpilot-fetch-json rp.session.*
    mv *.prof rp.session.* pipe-16-trial-$t
done
