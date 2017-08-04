export RADICAL_ENTK_VERBOSE=INFO
export RADICAL_ENTK_PROFILE=True
export RADICAL_PILOT_PROFILE=True
export RADICAL_PILOT_DBURL=mongodb://rp:rp@ds015335.mlab.com:15335/rp
export RP_ENABLE_OLD_DEFINES=True

mkdir app_sleep
mkdir app_sleep/pipes-2
python poe.py 2 sleep xsede.supermic
radicalpilot-fetch-json rp.session.*
mv *.prof rp.session.* app_sleep/pipes-2
