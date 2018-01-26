import os

__author__ = 'Landon T. Clipp'
__email__  = 'clipp2@illinois.edu'

PBS_DIR = os.path.join( os.path.dirname( __file__ ), 'pbs_files' )

max_pbs_num = 5000
'''maximum number of PBS files to store in PBS_DIR'''
max_pbs_age = 30
'''maximum age in days of PBS files in PBS_DIR'''

