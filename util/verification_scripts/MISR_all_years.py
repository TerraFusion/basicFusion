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

	
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2000.txt", "2000")
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2001.txt", '2001')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2002.txt", '2002')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2003.txt", '2003')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2004.txt", '2004')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2005.txt", '2005')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2006.txt", '2006')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2007.txt", '2007')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2008.txt", '2008')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2009.txt", '2009')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2010.txt", '2010')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2011.txt", '2011')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2012.txt", '2012')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2013.txt", '2013')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2014.txt", '2014')
	# check_Orbit(dirpath, "/home/sbansal6/File_Verification_Scripts/MISR_Check/MISR_Check_2015.txt", '2015')

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
	OrbFile = open(orbFile)
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

def check_Orbit(dirpath, logPath, year, orbFile):
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
		
		#to make a list of all the missing dates
		if date in dateStr:
			dateStr.remove(date)

			for fname in os.listdir(inPath):
				statinfo = os.stat(os.path.join(inPath, fname))

				if fname.endswith('.hdf') and statinfo.st_size != 0 and '_' in fname:
					pathOrbitNo = fname.split('_')
					orbitStr = filter(str.isdigit, pathOrbitNo[6])
					orbitDate =  check_OrbitFile_in_Correct_date(str(int(orbitStr)), orbFile)

					if(date == ('.'.join(orbitDate.split('-')))):
						camera = pathOrbitNo[7]
						orbitFiles[date + '/' + pathOrbitNo[5] + '_' + pathOrbitNo[6]].append(camera)
					else:
						actualDate = '.'.join(orbitDate.split('-'))
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
