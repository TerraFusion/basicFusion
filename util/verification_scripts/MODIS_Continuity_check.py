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
from datetime import datetime, timedelta

def main():

	dirPath2000 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2000'
	filename2000 = "MODIS_Continuity_Check_MOD021KM_2000.txt"
	
	dirPath2001 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2001'
	filename2001 = "MODIS_Continuity_Check_MOD021KM_2001.txt"
	
	dirPath2002 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2002'
	filename2002 = "MODIS_Continuity_Check_MOD021KM_2002.txt"
	
	dirPath2003 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2003'
	filename2003 = "MODIS_Continuity_Check_MOD021KM_2003.txt"
	
	dirPath2004 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2004'
	filename2004 = "MODIS_Continuity_Check_MOD021KM_2004.txt"
	
	dirPath2005 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2005'
	filename2005 = "MODIS_Continuity_Check_MOD021KM_2005.txt"
	
	dirPath2006 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2006'
	filename2006 = "MODIS_Continuity_Check_MOD021KM_2006.txt"
	
	dirPath2007 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2007'
	filename2007 = "MODIS_Continuity_Check_MOD021KM_2007.txt"
	
	dirPath2008 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2008'
	filename2008 = "MODIS_Continuity_Check_MOD021KM_2008.txt"
	
	dirPath2009 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2009'
	filename2009 = "MODIS_Continuity_Check_MOD021KM_2009.txt"
	
	dirPath2010 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2010'
	filename2010 = "MODIS_Continuity_Check_MOD021KM_2010.txt"
	
	dirPath2011 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2011'
	filename2011 = 'MODIS_Continuity_Check_MOD021KM_2011.txt'
	
	dirPath2012 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2012'
	filename2012 = 'MODIS_Continuity_Check_MOD021KM_2012.txt'
	
	dirPath2013 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2013'
	filename2013 = 'MODIS_Continuity_Check_MOD021KM_2013.txt'
	
	dirPath2014 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2014'
	filename2014 = 'MODIS_Continuity_Check_MOD021KM_2014.txt'
	
	dirPath2015 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2015'
	filename2015 = 'MODIS_Continuity_Check_MOD021KM_2015.txt'
	
	dirPath2016 = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM/2016'
	filename2016 = 'MODIS_Continuity_Check_MOD021KM_2016.txt'
	

	check_Continuity(dirPath2000, filename2000, 367)
	check_Continuity(dirPath2001, filename2001, 366)
	check_Continuity(dirPath2002, filename2002, 366)
	check_Continuity(dirPath2003, filename2003, 366)
	check_Continuity(dirPath2004, filename2004, 367)
	check_Continuity(dirPath2005, filename2005, 366)
	check_Continuity(dirPath2006, filename2006, 366)
	check_Continuity(dirPath2007, filename2007, 366)
	check_Continuity(dirPath2008, filename2008, 367)
	check_Continuity(dirPath2009, filename2009, 366)
	check_Continuity(dirPath2010, filename2010, 366)
	check_Continuity(dirPath2011, filename2011, 366)
	check_Continuity(dirPath2012, filename2012, 367)
	check_Continuity(dirPath2013, filename2013, 366)
	check_Continuity(dirPath2014, filename2014, 366)
	check_Continuity(dirPath2015, filename2015, 366)
	check_Continuity(dirPath2016, filename2016, 367)


def check_Continuity(dirPath, textfile, leap):
	exist = {}
	orbitFiles = defaultdict(list)

	checklist = []
	numlist = []
	emptydir = []
	hdfempty = []


	for i in range(1, leap):
		checklist.append(i)

	#this makes a list of all the dates than need to be checked
	with open("datetime.txt") as file:
		for line in file:
			numlist.append(line[0:4])

	#this is the text file where all the data is written 
	fileCheck = open(textfile, "w").close()
	fileCheck = open(textfile, "w")
	

	for fname in os.listdir(dirPath):

		#checks whether the directory in the particular year is empty
		if(os.listdir(os.path.join(dirPath, fname)) == []):
			emptydir.append(fname)
			checklist.remove(int(fname))


		else:
			checklist.remove(int(fname))
			missinglist = []
			numlist2 = []
			strlist = []

			#this makes sure that we do not enter an empty directory
			if fname not in emptydir:
				inPath = os.path.join(dirPath, fname)

				for filename in os.listdir(inPath):
					hdfinfo = os.stat(os.path.join(inPath, filename))

					#this makes sure that the file is in .hdf format and not empty
					if(filename.endswith('.hdf') and hdfinfo.st_size != 0):
						strlist = filename.split('.')
						numlist2.append(strlist[2])

					#this makes a list of all the empty hdf files
					else: 
						hdfempty.append(os.path.join(inPath, filename))

				#this makes a list of all the missing continuity
				#there should be data for every 5 min starting from 00:00 till 23:55
				missinglist = list(set(numlist) - set(numlist2))
				if missinglist and strlist:
					fileCheck.write(inPath + "\n")
					for miss in range(len(missinglist)):
						fileCheck.write(strlist[0] + "." + strlist[1] + "." + missinglist[miss] + "." + strlist[3] + "\n")

					fileCheck.write('\n')

	
	#writes to the text file if and day is missing from the year
	fileCheck.write("The following are the missing days: \n")
	for i in range(len(checklist)):
		fileCheck.write(os.path.join(dirPath, str(checklist[i])) + "\n") 

	fileCheck.write('\n')
	
	#writes to the text file if there is an empty date in the year
	if emptydir:
		fileCheck.write("The following directories are empty: \n")
		for i in range(len(emptydir)):
			fileCheck.write(os.path.join(dirPath, emptydir[i]) + "\n")

	fileCheck.write('\n')

	#writes to the text file if any hdf file is empty
	if hdfempty:
		fileCheck.write("The following hdf files are empty (zero size): \n")
		for i in range(len(hdfempty)):
			fileCheck.write(hdfempty[i] + '\n')

main()

