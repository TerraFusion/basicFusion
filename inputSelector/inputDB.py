from __future__ import print_function
import sqlite3
from logging import *

def createOrbits(cursor, filename):
    c.execute('CREATE TABLE orbits (startTime integer PRIMARY KEY,  endTime integer, orbit integer)')
    # Orbit data
    timeFmt = '%Y-%m-%dT%H:%M:%SZ'
    with open(infile) as f:
        for line in f:
            orbitnum, starttime, endtime = line.split()
            stime = datetime.strptime(starttime, timeFmt)
            etime = datetime.strptime(endtime, timeFmt)
            c.execute("INSERT OR IGNORE INTO {tn} ({stime}, {etime}, {orbit}) VALUES (stime, etime, orbitnum)".\
                format(tn='orbits', stime='startTime', etime='endTime', orbit='orbit')
    return

def main():
    eval sys.argv[2:]   # set the vars from commandline
    

if __name__ == '__main__':
    logLevel = logging.NOTSET
    if 'LOGLEVEL' in os.environ:
        logLevel = getattr(logging, os.environ['LOGLEVEL'])
    logging.basicConfig(level=logLevel, stream=sys.stdout, 
	format=sys.argv[0].split('/')[-1]+': %(message)s')
    main()


def createDB(dbfile):
    conn = sqlite3.connect(file)
    c = conn.cursor()

    createOrbits(c)

    c.execute('CREATE TABLE orbits (startTime integer PRIMARY KEY,  endTime integer, orbit integer)')
    c.execute('CREATE TABLE aster (file text) (startTime integer PRIMARY KEY, endTime integer)')
    c.execute('CREATE TABLE modis (file text) (startTime integer PRIMARY KEY) (endTime integer)')
    c.execute('CREATE TABLE mopitt (file text) (startTime integer PRIMARY KEY) (endTime integer)')
    c.execute('CREATE TABLE ceres (file text) (startTime integer PRIMARY KEY) (endTime integer)')
    c.execute('CREATE TABLE orbits (orbit integer) (startTime integer PRIMARY KEY) (endTime integer)')
    c.execute('CREATE TABLE aster (file text) (startTime integer PRIMARY KEY) (endTime integer)')
    c.execute('CREATE TABLE modis (file text) (startTime integer PRIMARY KEY) (endTime integer)')
    c.execute('CREATE TABLE mopitt (file text) (startTime integer PRIMARY KEY) (endTime integer)')
    c.execute('CREATE TABLE ceres (file text) (startTime integer PRIMARY KEY) (endTime integer)')

    # Orbit data
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



def connect(dbfile):
    conn = sqlite3.connect(file)
    c = conn.cursor()


    # Table:
    # FILE, STIME, ETIME, INSTRUMENT
    # TEXT, INTEGER, INTEGER, TEXT

    TEXT, INTEGER, INTEGER, TEXT






DB_NAME = 'basic_fusion_files'

TABLES = {}
TABLES['aster'] = (
    "CREATE TABLE `aster` ("
    "  `file_name` varchar(128) NOT NULL,"
    "  `start_date` date NOT NULL,"
    "  `end_date` date NOT NULL,"
    "  PRIMARY KEY (`start_date`), KEY `end_date` (`end_date`),"
    ") ENGINE=InnoDB")

TABLES['ceres'] = (
    "CREATE TABLE `ceres` ("
    "  `file_name` varchar(128) NOT NULL,"
    "  `start_date` date NOT NULL,"
    "  `end_date` date NOT NULL,"
    "  PRIMARY KEY (`start_date`), KEY `end_date` (`end_date`),"
    ") ENGINE=InnoDB")

TABLES['misr'] = (
    "CREATE TABLE `misr` ("
    "  `file_name` varchar(128) NOT NULL,"
    "  `start_date` date NOT NULL,"
    "  `end_date` date NOT NULL,"
    "  PRIMARY KEY (`start_date`), KEY `end_date` (`end_date`),"
    ") ENGINE=InnoDB")

TABLES['modis'] = (
    "CREATE TABLE `dept_emp` ("
    "  `file_name` varchar(128) NOT NULL,"
    "  `start_date` date NOT NULL,"
    "  `end_date` date NOT NULL,"
    "  PRIMARY KEY (`start_date`), KEY `end_date` (`end_date`),"
    ") ENGINE=InnoDB")

TABLES['moppit'] = (
    "  CREATE TABLE `dept_manager` ("
    "  `file_name` varchar(128) NOT NULL,"
    "  `start_date` date NOT NULL,"
    "  `end_date` date NOT NULL,"
    "  PRIMARY KEY (`start_date`), KEY `end_date` (`end_date`),"
    ") ENGINE=InnoDB")

