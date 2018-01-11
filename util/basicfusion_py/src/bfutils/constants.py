import os 

__author__ = 'Landon T. Clipp'
__email__  = 'clipp2@illinois.edu.'

ROOT_DIR = os.path.dirname( os.path.abspath( __file__ ) )
ORBIT_INFO_TXT = os.path.join( ROOT_DIR, 'Orbit_Path_Time.txt' )
ORBIT_INFO_DICT = os.path.join( ROOT_DIR, 'Orbit_Path_Time.p' )
LOG_FMT='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
LOG_DATE_FMT='%d-%m-%Y:%H:%M:%S'
