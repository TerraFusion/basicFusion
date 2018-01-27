'''
Parse the Orbit_Path_Time.txt file and generate a corresponding
Orbit_Path_Time.json file. json has the following format:

{
    [orbit num] : 
    {
        path : [path_num],
        stime : [start time],
        etime : [end time]
    }       
}
'''

import json
import sys
import datetime

# Path of the Orbit_Path_Time.txt file
o_txt = './Orbit_Path_Time.txt'

json_dict = {}
with open( o_txt, 'r' ) as f:
    for line in f:
        split = line.split(' ')
        orbit = split[0].strip()
        path = split[1].strip()
        stime = split[2].strip()
        etime = split[3].strip()

        d = datetime.datetime.strptime(stime, '%Y-%m-%dT%H:%M:%SZ')
        stime = d.strftime('%Y%m%d%H%M%S')
        d = datetime.datetime.strptime(etime, '%Y-%m-%dT%H:%M:%SZ')
        etime = d.strftime('%Y%m%d%H%M%S')
        json_dict[orbit] = { 'path' : path, 'stime' : stime, 'etime' : etime }

with open( 'Orbit_Path_Time.json', 'wb' ) as f:
    #json.dump( json_dict, f, sort_keys=True, indent=4, separators=(',', ': ') )
    json.dump( json_dict, f, separators=(',', ':') )
