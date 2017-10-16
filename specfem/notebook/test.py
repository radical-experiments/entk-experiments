import sys
import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
from radical.entk import Profiler
import radical.analytics as ra
import radical.utils as ru
import radical.pilot as rp
import numpy as np
from math import sqrt
import os
#from __future__ import unicode_literals
#from IPython.core.display import display, HTML
import matplotlib as mpl
os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://user:user@ds117605.mlab.com:17605/ssflow-2'

def get_task_uids(num_tasks):
    
    task_uids = []
    for t in range(num_tasks):
        task_uids.append('radical.entk.task.%04d'%t)

    return task_uids

if __name__ == '__main__':

    src = sys.argv[1]

    # find json file in dir, and derive session id
    json_files = glob.glob('%s/*.json' % src)

    if len(json_files) < 1: raise ValueError('%s contains no json file!' % src)
    if len(json_files) > 1: raise ValueError('%s contains more than one json file!' % src)

    json_file = json_files[0]
    json      = ru.read_json(json_file)
    sid       = os.path.basename(json_file)[:-5]

    print 'sid: %s' % sid

    session = ra.Session(sid, 'radical.pilot', src=src)
    units = session.filter(etype='unit', inplace=False)
    pilots = session.filter(etype='pilot', inplace=False)

    rp_dur = units.duration([rp.UMGR_SCHEDULING, rp.DONE])
    exec_dur = units.duration([rp.AGENT_EXECUTING, rp.AGENT_STAGING_OUTPUT_PENDING])

    pilot_dur = pilots.duration([rp.PMGR_ACTIVE, rp.FINAL])

    print 'Pilot: %s, RP: %s, Exec: %s'%(pilot_dur, rp_dur, exec_dur)


    tasks = [1]
    trials=1
    data_loc = '../raw_data/serial_execution/events_1/'
    
    df = pd.DataFrame(columns=[ 'EnTK setup overhead','EnTK teardown overhead', 'RTS cancelation overhead', 
                               'EnTK task management overhead', 'RTS overhead', 'Execution time'])
    df_err = pd.DataFrame(columns=[ 'EnTK setup overhead','EnTK teardown overhead', 'RTS cancelation overhead', 
                               'EnTK task management overhead', 'RTS overhead', 'Execution time'])

    tasks = [1]

    for task in tasks:
    
        entk_setup_ov_list = list()
        entk_teardown_ov_list = list()
        entk_rp_cancel_ov_list = list()
        entk_task_mgmt_ov_list = list()
        rp_ov_list = list()
        exec_list = list()
    
        for t in range(1,trials+1):
       
            src = glob.glob('{0}/rp.session*'.format(data_loc,task))[0]
            print src
        
            json_files = glob.glob('{0}/*.json'.format(src))
            json_file = json_files[0]
            json      = ru.read_json(json_file)
            sid       = os.path.basename(json_file)[:-5]

            session = ra.Session(sid, 'radical.pilot', src=src)
            units = session.filter(etype='unit', inplace=False)
            pilots = session.filter(etype='pilot', inplace=False)
    
            for unit in units.get()[:1]:
                print unit.states
    
            p = Profiler(src = src + '/..')
        
            task_uids = get_task_uids(task)
        
            entk_dur = p.duration(task_uids, states=['SCHEDULING', 'DONE'])
    
            rp_dur = units.duration([rp.UMGR_SCHEDULING_PENDING, rp.DONE])
            exec_dur = units.duration([rp.AGENT_EXECUTING, rp.AGENT_STAGING_OUTPUT_PENDING])
        
            pilot_dur = pilots.duration([rp.PMGR_ACTIVE, rp.FINAL])
    
            print 'Pilot: %s, RP: %s, Exec: %s'%(pilot_dur, rp_dur, exec_dur)
