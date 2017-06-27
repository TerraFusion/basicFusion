from os import path
import os
import fnmatch
from collections import defaultdict
import collections
import mmap
import sys


def main():
	# arg = sys.argv[1:]
	# dirpath = (arg[0])
	# logPath = (arg[1])
	# year = str(arg[2])

	dirpath = '/projects/TDataFus/gyzhao/TF/data/MISR/MI1B2E.003'

	
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2000.txt", 2000)
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2001.txt", '2001')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2002.txt", '2002')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2003.txt", '2003')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2004.txt", '2004')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2005.txt", '2005')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2006.txt", '2006')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2007.txt", '2007')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2008.txt", '2008')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2009.txt", '2009')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2010.txt", '2010')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2011.txt", '2011')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2012.txt", '2012')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2013.txt", '2013')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2014.txt", '2014')
	check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2015.txt", '2015')

def check_OrbitFile_in_Correct_date(orbitNo):
	OrbFile = open("/home/sbansal6/File_Verification_Scripts/Orbit_Path_Time.txt")
	s = mmap.mmap(OrbFile.fileno(), 0, access=mmap.ACCESS_READ)
	linelist = []
	if s.find(orbitNo) != -1: 
		indexNo = s.find(orbitNo)
		s.seek(indexNo)
		line = s.readline()
		linelist = line.split(" ")
		if(len(linelist) > 2):
			return linelist[2][0:10]
		elif(len(linelist) == 2):
			return linelist[0][0:10]
		else:
			print linelist

def check_Orbit(dirpath, logPath, year):
	leap = ['2000','2004','2008','2012','2016']
	exist = {}
	wrongDate = {}
	cameralist = ['AA', 'AF', 'AN', 'BA', 'BF', 'CA', 'CF', 'DA', 'DF']
	
	fileCheck = open(logPath, "w").close()
	fileCheck = open(logPath, "w")

	orbitFiles = defaultdict(list) #makes a list of all the missing cameras
	dateStr = []

	# fileCheck = open("MISR_date_Check.txt", "w").close()
	# fileCheck = open("MISR_date_Check.txt", "w")
	
	#makes a list of all the dates in a year
	for m in range(1, 13):
		for d in range(1, 31):

			if(m < 10 and d < 10):
				dateStr.append(year + '.0' + str(m) + '.0' + str(d))

			elif(m<10 and d >= 10):
				dateStr.append(year + '.0' + str(m) + '.' + str(d))

			elif(m>=10 and d < 10):
				dateStr.append(year + '.' + str(m) + '.0' + str(d))
			else:
				dateStr.append(year + '.' + str(m) + '.' + str(d))

	#print(dateStr)
	#print(os.listdir(dirpath))

	for date in os.listdir(dirpath):
		#path to each date
		inPath = os.path.join(dirpath,date)
		
		#to make a list of all the missing dates
		if date in dateStr:
			dateStr.remove(date)

			for fname in os.listdir(inPath):
				statinfo = os.stat(os.path.join(inPath, fname))

				if fname.endswith('.hdf') and statinfo.st_size != 0 and '_' in fname:
					pathOrbitNo = fname.split('_')
					#print(pathOrbitNo)
					orbitStr = filter(str.isdigit, pathOrbitNo[6])
					orbitDate =  check_OrbitFile_in_Correct_date(str(int(orbitStr)))

					if(date == ('.'.join(orbitDate.split('-')))):
						camera = pathOrbitNo[7]
						orbitFiles[date + '/' + pathOrbitNo[5] + '_' + pathOrbitNo[6]].append(camera)
					else:
						actualDate = '.'.join(orbitDate.split('-'))
						#print(actualDate)
						fileCheck.write(fname + " is in " + date + " should be in " + actualDate + ".\n")


	if orbitFiles:
		fileCheck.write("\n These are the missing files: \n")
		for orbit in orbitFiles:
			if (len(set(cameralist) - set(orbitFiles[orbit])) != 0):
				missing = list(set(cameralist) - set(orbitFiles[orbit]))
				for i in range(len(missing)):
					fileCheck.write('MISR/MI1B2E.003' + '/' + orbit[0:10] + '/MISR_AM1_GRP_ELLIPSOID_GM_' + orbit[10:] + '_' + missing[i] + '_F03_0024.hdf')
					fileCheck.write("\n")


	if (year + ".02.30") in dateStr:
		dateStr.remove(year + ".02.30")

	if (year + ".02.31") in dateStr:
		dateStr.remove(year + ".02.31")

	if (year + ".04.31") in dateStr:
		dateStr.remove(year + ".04.31")

	if (year + ".06.31") in dateStr:
		dateStr.remove(year + ".06.31")

	if (year + ".09.31") in dateStr:
		dateStr.remove(year + ".09.31")

	if (year + ".11.31") in dateStr:
		dateStr.remove(year + ".11.31")
	
	if year not in leap and (year + ".02.29") in dateStr:
		dateStr.remove(year + ".02.29")



	if dateStr:
		fileCheck.write("\nThe missing dates are: \n")
		for i in range(len(dateStr)):
			fileCheck.write(dateStr[i] + "\n")


main()
