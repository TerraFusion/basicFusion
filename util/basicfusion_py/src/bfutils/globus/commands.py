from bfutils.globus import constants
import subprocess
import os

def ls( id, path, recursive=False):
    if type(id) is not str:
        raise TypeError('id is not a string.')
    if type(path) is not str:
        raise TypeError('path is not a string.')

    arg = ['globus', 'ls', '{}:{}'.format(id, path)]
    if recursive:
        arg.append('--recursive')

    # Split the returned string by line, returning list of strings
    ret_str = subprocess.check_output( arg ).split()
    for i in range( len(ret_str ) ):
        ret_str[i] = os.path.join( path, ret_str[i] )
    return ret_str
