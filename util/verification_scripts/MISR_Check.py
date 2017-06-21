"""
@author : Shashank Bansal
@email : sbansal6@illinois.edu

"""

from os import path
import os
import fnmatch
from collections import defaultdict
import collections
import mmap
import cProfile
import re
import pstats

dirpath = '/projects/TDataFus/BFData/MISR/MI1B2E.003'

def check_OrbitFile_in_Correct_date(orbitNo):
	OrbFile = open("Orbit_Path_Time.txt")
	s = mmap.mmap(OrbFile.fileno(), 0, access=mmap.ACCESS_READ)
	if s.find(orbitNo) != -1: 
		indexNo = s.find(orbitNo)
		s.seek(indexNo)
		line = s.readline()
		return line[10:20]
	
	# for line in OrbFile:
	# 	if orbitNo in line:
	# 		return line[10:20]

		


def check_Orbit():
	exist = {}
	orbitFiles = defaultdict(list)
	wrongDate = {}
	cameralist = ['AA', 'AF', 'AN', 'BA', 'BF', 'CA', 'CF', 'DA', 'DF']
	file = open("MISR_check2.txt", "w").close()
	file = open("MISR_check2.txt", "w")

	fileCheck = open("MISR_date_Check.txt", "w").close()
	fileCheck = open("MISR_date_Check.txt", "w")
	for m in range(1, 13):
	    for d in range(1, 31):
	        if(m < 10 and d < 10):
	            dateStr = '2013' + '.0' + str(m) + '.0' + str(d)
	            dateOrb = '2013' + '-0' + str(m) + '-0' + str(d)
	        elif(m<10 and d >= 10):
	            dateStr = '2013' + '.0' + str(m) + '.' + str(d)
	            dateOrb = '2013' + '-0' + str(m) + '-' + str(d)
	        elif(m>=10 and d < 10):
	            dateStr = '2013' + '.' + str(m) + '.0' + str(d)
	            dateOrb = '2013' + '-' + str(m) + '-0' + str(d)
	        else:
	            dateStr = '2013' + '.' + str(m) + '.' + str(d)
	            dateOrb = '2013' + '-' + str(m) + '-' + str(d)
	        
	        #print dateStr
	        inPath = os.path.join(dirpath, dateStr)
	        if not path.exists(inPath):
	            exist[ 'MISR/MI1B2E.003' + '/' + dateStr] = 'Date Doesnot Exist'
	        else:
	        	#seperates the different cameras files that exist for each orbit no. by making a dictionary of the list of all the 
	        	#cameras files for each orbit number in each particular date file
	            for fname in os.listdir(inPath):
	            	statinfo = os.stat(os.path.join(inPath, fname))
	            	if(fname.endswith('.hdf') and statinfo.st_size != 0):
	            		pathOrbitNo = fname[26:38]
	            		orbitDate =  check_OrbitFile_in_Correct_date(pathOrbitNo[7:12])
	            		#print(orbitDate)
	            		if(orbitDate == dateOrb):
		            		camera = fname[39:41]
		            		orbitFiles[pathOrbitNo].append(camera)

		            	else:
		            		#wrongDate[fname] = orbitDate
		            		fileCheck.write(fname + " is in " + dateOrb + " should be in " + orbitDate + ".\n")


		            	# camera = fname[39:41]
		            	# orbitFiles[pathOrbitNo].append(camera)

	        """for check in orbitFiles:
	        	print check, orbitFiles[check]"""
	        #checking if for each orbit number there exists all the cameras

	        for orbit in orbitFiles:
	        	if (len(set(cameralist) - set(orbitFiles[orbit])) != 0):
	        		missing = list(set(cameralist) - set(orbitFiles[orbit]))
	        		#print(orbitFiles[orbit])
	        		#print(missing)
	        		for i in range(len(missing)):
	        			file.write('MISR/MI1B2E.003' + '/' + dateStr + '/MISR_AM1_GRP_ELLIPSOID_GM_' + orbit + '_' + missing[i] + '_F03_0024.hdf')
	        			file.write("\n")
	        			#exist['MISR/MI1B2E.003' + '/' + dateStr + '/MISR_AM1_GRP_ELLIPSOID_GM_' + orbit + '_' + missing[i] + '_F03_0024.hdf' ] = 'File Missing'
	        
	        orbitFiles.clear()
	        #wrongDate.clear()

	file.close()
	fileCheck.close()
	#special cases for the months with 31st    
	if not path.exists(os.path.join(dirpath, '2013.01.31')):
	    exist[ 'MISR/MI1B2E.003' + '/' + '2013.01.31'] = 'Date Doesnot Exist'

	if not path.exists(os.path.join(dirpath, '2013.03.31')):
	    exist[ 'MISR/MI1B2E.003' + '/' + '2013.03.31'] = 'Date Doesnot Exist'

	if not path.exists(os.path.join(dirpath, '2013.05.31')):
	    exist[ 'MISR/MI1B2E.003' + '/' + '2013.05.31'] = 'Date Doesnot Exist'

	if not path.exists(os.path.join(dirpath, '2013.07.31')):
	    exist[ 'MISR/MI1B2E.003' + '/' + '2013.07.31'] = 'Date Doesnot Exist'

	if not path.exists(os.path.join(dirpath, '2013.08.31')):
	    exist[ 'MISR/MI1B2E.003' + '/' + '2013.08.31'] = 'Date Doesnot Exist'

	if not path.exists(os.path.join(dirpath, '2013.10.31')):
	    exist[ 'MISR/MI1B2E.003' + '/' + '2013.10.31'] = 'Date Doesnot Exist'

	if not path.exists(os.path.join(dirpath, '2013.12.31')):
	    exist[ 'MISR/MI1B2E.003' + '/' + '2013.12.31'] = 'Date Doesnot Exist'

	#deleting extra days from the month of Feb
	del exist['MISR/MI1B2E.003' + '/' + '2013.02.29']
	del exist['MISR/MI1B2E.003' + '/' + '2013.02.30']

	#cProfile.run('re.compile("foo|bar")')
	#cProfile.run('re.compile("foo|bar")', 'restats')
	for i in exist:
		print(i, exist[i])

def main():
	#print check_OrbitFile_in_Correct_date("70277")
	check_Orbit()

	cProfile.run('re.compile("foo|bar")', 'restats')

main()
"""file = open("MISR_check1.txt", "w").close()
file = open("MISR_check1.txt", "w")
for i in exist:
    if (exist[i] == 'Data Doesnot Exist' or exist[i] == 'File Missing'):
        file.write(i)
        file.write("\n")
file.close()"""

