#!/bin/bash
export RADICAL_ENTK_VERBOSE=INFO
export RADICAL_ENTK_PROFILE=True
export RADICAL_PILOT_PROFILE=True
export RADICAL_PILOT_DBURL=mongodb://user:user@ds117615.mlab.com:17615/test-10-12
export RP_ENABLE_OLD_DEFINES=True

for t in $(seq 3 1 5); do
    mkdir raw_data/gromacs-250-trial-$t
    python poe.py 16 xsede.supermic
    radicalpilot-fetch-json rp.session.*
    mv *.prof rp.session.* raw_data/gromacs-250-trial-$t
done
