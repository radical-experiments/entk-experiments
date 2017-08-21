import radical.pilot as rp
import radical.analytics as ra
import radical.utils as ru
import sys, os, glob
import pprint

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

    pilots = session.filter(etype='pilot', inplace=False)
    durations = pilots.duration([rp.PMGR_ACTIVE, rp.FINAL])
    pprint.pprint(durations)
