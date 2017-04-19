#
# Print a table of various orbit times and their frequencies from a file
# of orbit records
#
import time
import sys
from datetime import datetime
import from logging import debug, info

infile = 'orbittotimelist.txt'
#infile = 'orbit-test.txt'

timeFmt = '%Y-%m-%dT%H:%M:%SZ'
orbits = {}
prev_dur_diff = None
prev_orbit_change_no = None
with open(infile) as f:
    for line in f:
        orbitnum, starttime, endtime = line.split()
        stime = datetime.strptime(starttime, timeFmt)
        etime = datetime.strptime(endtime, timeFmt)
        duration = etime-stime
        orbits[duration] = orbits.get(duration, 0) +1
    for k in sorted(orbits):
        print ('Orbit duration {}: count {}'.format(k, orbits[k]))
    #for d, c in orbits.iteritems():
        #print 'duration {}: count: {}'.format(d, c)
'''        if duration != prev_dur_diff:
            if prev_dur_diff is not None:
                print 'orbit {} changed since orbit {}: duration changed to {} from previous {}'.format(orbitnum, prev_orbit_change_no, etime-stime, prev_dur_diff)
            prev_dur_diff = duration
            prev_orbit_change_no = orbitnum
#for o in orbits.keys():
   # print orbits[o][1] - orbits[o][0]'''

if __name__ == '__main__':
    logLevel = logging.NOTSET
    if 'PSI_CLIENT_DEBUG' in os.environ:
        logLevel = getattr(logging, os.environ['PSI_CLIENT_DEBUG'])
    if 'RMQ_DEBUG' in os.environ:
        rmqLogLevel = getattr(logging, os.environ['RMQ_DEBUG'])
    logging.basicConfig(level=logLevel, stream=sys.stdout, 
	format=sys.argv[0].split('/')[-1]+': %(message)s')
    runhello()
    

print('bye')

def __main__()
