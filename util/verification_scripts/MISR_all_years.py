"""
@author : Shashank Bansal
@email : sbansal6@illinois.edu

Date Created: June 12, 2017
"""

from os import path
import os
import fnmatch
from collections import defaultdict
import collections
import mmap
import sys


def main():

	if not len(sys.argv) > 1:
		usage()

	elif sys.argv[1] == '-h' or sys.argv[1] == '-help' :
		help()

	elif sys.argv[1] == '-a':
		arguments()

	elif sys.argv[1] == '-r':
		if len(sys.argv) != 6:
			arguments()
		else:
			dirpath = sys.argv[2]
			logfile = sys.argv[3]
			year = sys.argv[4]
			orbFile = sys.argv[5]
			run(dirpath, logfile, year, orbFile)
	else:
		usage()

	# arg = sys.argv[1:]
	# dirpath = (arg[0])
	# logPath = (arg[1])
	# year = str(arg[2])

	# dirpath = '/projects/TDataFus/gyzhao/TF/data/MISR/MI1B2E.003'
	#orbFile = ''


def usage():
	print("\n\nUSAGE:  python MISR_all_years.py [-h | -a | -r]\n")
	print("Options:\n")
	print("-h     									More information about the different functions and how they work\n")
	print("-a           								More information about the arguments and the format to run the program\n")
	print("-r [dirpath] [logfile] [year] [orbFile]  				Run the program\n")
	#print("-m           						Run the program on multiple years at once.")


def help():
	print("\nDESCRIPTION:\n\nThere are 2 main functions in this program- ")
	print("1. check_Orbit(dirpath, logPath, year, orbFile)")
	print("2. check_OrbitFile_in_Correct_date(orbitNo, orbFile)\n")
	print("1. check_Orbit(dirpath, logPath, year, orbFile):")
	print("MISR should have 9 camera files for each orbit. This function checks and finds the missing camera files")
	print("for each orbit for the given year. It also makes a list of all the orbitFiles in the wrong date. The")
	print("orbFile contains the right dates for each orbitNo.\n")
	print("2. check_Orbit_in_Correct_date(orbitNo, orbFile):\nThis function checks if the orbitFiles are in the correct date.\n")

def arguments():
	print("\nARGUMENTS TO RUN PROGRAM:\n\n python MISR_all_years.py -r [absolute/path/to/dir] [filepath/to/logfile] [year] [absolute/path/to/orbFile]\n")
	print("For more information on each argument, type i.")
	info = str(raw_input())
	
	if info == 'i' or info == 'I':
		print("MORE INFORMATION: \n\nFor this program to run properly, you need 4 arguments. All of the arguments are for check_Orbit.\n")
		print("1. The first is the directory path. For example, it should look something like:")
		print("\npath/to/dir or /projects/TDataFus/gyzhao/TF/data/MISR/MI1B2E.003\n")
		print("In order for the program to run smoothly, you need to provide the absolute path and make sure that you")
		print("provide the dirpath only uptil just before the dates.\n")
		
		print("2. The second argument is the file path to the logfile where you wish to write the names of the missing files.")
		print("For example, it should look something like,")
		print("\n/path/to/logfile/logfile.txt or /home/user/scratch/BasicFusion/MISR_logfiles/logfile.txt\n")
		print("The second argument also need to have the name of the .txt file where you wish to log all the missing files.")
		print("You don't need to make a TextFile, just give the name in the second argument and the program will make a txtfle")
		print("with that name in the specified directory.\n")
		
		print("3. The third argument is the year you wish to run the program on. The range of years that the database currently")
		print("has is between 2000 to 2016.\n")
		
		print("4. The fourth argument is the path to the orbit_dateTime or the orbFile file. It should be a txt file that")
		print("contains all the orbits with the dates they are suppose to be in. For example,")
		print("\npath/to/orbFile or /home/user/File_Verification_Scripts/scripts/Orbit_Path_Time.txt\n")

	else:
		return

def run(dirpath, logfile, year, orbFile):

	check_Orbit(dirpath, logfile, year, orbFile)



def check_OrbitFile_in_Correct_date(orbitNo, orbFile):

	""" DESCRIPTION:
		This function checks if the orbit file is in the correct date. 

		ARGUMENTS:
		orbitNo (int)		-- the orbit number that needs to be checked.
		orbFile (txt file)  -- the text file that contains all the orbit numbers and the dates they are suppose to be in.

		EFFECTS:

		RETURN:
			Returns the starting date of the particular orbitNo passed in as an argument if it exists in the txt file else 
			prints an error message. 

	"""
	#opens the txt file
	OrbFile = open(orbFile)
	s = mmap.mmap(OrbFile.fileno(), 0, access=mmap.ACCESS_READ)
	linelist = []


	#checks if the orbitNo exists in the file
	if s.find(orbitNo) != -1:
		indexNo = s.find(orbitNo)
		s.seek(indexNo)
		line = s.readline()

		#makes a list of all the text for the particular orbitNo
		linelist = line.split(" ")

		#returns the actual starting date for the particular orbitNo
		if(len(linelist) > 2):
			return linelist[2][0:10]
		elif(len(linelist) == 2):
			return linelist[0][0:10]
		else:
			print linelist

	else:
		print "ERROR: Orbit doesnot exist in the textfile.\n"


def check_Orbit(dirpath, logPath, year, orbFile):

	""" DESCRIPTION:
			This function checks if all the hdf files are in the correct date directory and finds all the missing 
			files from the MISR directory. A file is missing if any of the 9 camera file for any orbitNo is missing
			A date is missing if it doesn't exist in the MISR data.


		ARGUMENTS:
			dirpath (str)		-- path to the directory where the MISR data is contained
			logPath (str)		-- path to the log textfile where the missing hdf files will be written
			year (str)			-- the year for which the MISR data needs to be checked
			orbFile (str)		-- this contains the info about all the orbitNo and their actual starting date and ending date
		

		EFFECTS:
			None

		
		RETURN:
			Nothing. This writes all the information to a text file which is provided by the user.
			

	"""

	leap = ['2000','2004','2008','2012','2016']
	exist = {}
	wrongDate = {}
	cameralist = ['AA', 'AF', 'AN', 'BA', 'BF', 'CA', 'CF', 'DA', 'DF']
	
	fileCheck = open(logPath, "w").close()
	fileCheck = open(logPath, "w")

	orbitFiles = defaultdict(list)
	dateStr = []

	
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


	for date in os.listdir(dirpath):
		#path to each date
		inPath = os.path.join(dirpath,date)
		
		#to make a list of all the missing dates by removing the dates that exits from the list of all the dates
		if date in dateStr:
			dateStr.remove(date)

			#this is particular for the format of data in ROGER
			#this opens each date which should contain all the hdf files for that date
			for fname in os.listdir(inPath):
				statinfo = os.stat(os.path.join(inPath, fname))

				#checks for only the non-zero size hdffiles
				if fname.endswith('.hdf') and statinfo.st_size != 0 and '_' in fname:
					pathOrbitNo = fname.split('_')

					#finds the pathOrbitNo (int) for each hdffile
					orbitStr = filter(str.isdigit, pathOrbitNo[6])
					orbitDate =  check_OrbitFile_in_Correct_date(str(int(orbitStr)), orbFile)

					# checks if the actual date and the date of the orbit are the same
					if(date == ('.'.join(orbitDate.split('-')))):

						#makes a list of all the cameras that exist for that particular orbitNo
						camera = pathOrbitNo[7]
						orbitFiles[date + '/' + pathOrbitNo[5] + '_' + pathOrbitNo[6]].append(camera)
					
					#if the file is in a different date directory than its supposed to be in then it writes both the dates
					#in the logfile 
					else:
						actualDate = '.'.join(orbitDate.split('-'))
						fileCheck.write(fname + " is in " + date + " should be in " + actualDate + ".\n")

	#finds all the missing cameras for the orbitNo and writes all the missing camera files to the logfile
	if orbitFiles:
		fileCheck.write("\n These are the missing files: \n")
		for orbit in orbitFiles:
			if (len(set(cameralist) - set(orbitFiles[orbit])) != 0):
				missing = list(set(cameralist) - set(orbitFiles[orbit]))
				for i in range(len(missing)):
					fileCheck.write('MISR/MI1B2E.003' + '/' + orbit[0:10] + '/MISR_AM1_GRP_ELLIPSOID_GM_' + orbit[10:] + '_' + missing[i] + '_F03_0024.hdf')
					fileCheck.write("\n")

	#this removes the extra dates that were additionally added to the list of all the dates
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
	
	#special case for a leap year
	if year not in leap and (year + ".02.29") in dateStr:
		dateStr.remove(year + ".02.29")

	#writes all the missing dates to the logfile
	if dateStr:
		fileCheck.write("\nThe missing dates are: \n")
		for i in range(len(dateStr)):
			fileCheck.write(dateStr[i] + "\n")


main()
