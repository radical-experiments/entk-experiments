export RADICAL_ENTK_VERBOSE=True
export RADICAL_ENTK_PROFILE=True
export RADICAL_PILOT_PROFILE=True


mkdir app_sleep
mkdir app_sleep/tasks-2
python poe.py 2 sleep
mv *.prof app_sleep/tasks-2
