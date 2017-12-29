import os

from bfutils.orbit import *
import bfutils.globus as globus

# Check for some necessary conditions on import
from bfutils.constants import ORBIT_INFO_TXT as orbit_txt
if not os.path.isfile( orbit_txt ):
    raise RuntimeError('{} was not found. Copy the proper file to this location and try again.'.format( orbit_txt ))

