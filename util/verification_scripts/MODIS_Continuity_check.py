"""
@author : Shashank Bansal
@email : sbansal6@illinois.edu

"""

import os
from collections import defaultdict
import collections
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
			dateTime = sys.argv[5]
			run(dirpath, logfile, year, dateTime)
	else:
		usage()

def usage():
	print("\n\nUSAGE:  python MODIS_Continuity_Check.py [-h | -a | -r]\n")
	print("Options:\n")
	print("-h 		    					More information about the different functions and how they work\n")
	print("-a           						More information about the arguments and format\n")
	print("-r [dirpath] [logfile] [year] [dateTime] 		Run the program\n")


def help():
	print("\nThere is one main function in this program.\n")

	print("1. Continuity_Check(dirpath, logfile, year, dateTime): \nThis function is used to check if their is any discontinuity/missing files. The way a file is logged is, ")
	print("if there is gap of more than the interval of 5 minutes between 2 files. For example, Lets say that following")
	print("are some of the files in the directory, /projects/TDataFus/gyzhao/TF/data/MODIS/MOD03/2015/056\n")
	print("MOD03.A2015056.1340.006")
	print("MOD03.A2015056.1350.006")
	print("MOD03.A2015056.1355.006")
	print("\nHere MOD03.A2015056.1345.006 is missing file. In MODIS, data is collected at an interval of every 5 minutes starting from")
	print("0000 to 2355.\n")

def arguments():
	print("\nARGUMENTS TO RUN PROGRAM:\n\n python MODIS_Continuity_Check.py -r [absolute/path/to/dir] [filepath/to/logfile] [year] [path/to/dateTime.txt]\n")
	print("For more information on each argument, type i.")
	info = str(raw_input())

	if(info == 'i' or info =='I'):
		print("MORE INFORMATION:\n\nFor this program to run properly, you need 4 different arguments.")
		print("1. The first is the directory path. For example, it should look something like:")
		print("\npath/to/dir or /projects/TDataFus/gyzhao/TF/data/MODIS/MOD03\n")
		print("In order for the program to run smoothly, you need to provide the absolute path and make sure that you")
		print("provide the dirpath only uptil just before the year.\n")
		
		print("2. The second argument is the file path to the logfile where you wish to write the names of the missing files.")
		print("For example, it should look something like,")
		print("\n/path/to/logfile/logfile.txt or /home/user/scratch/BasicFusion/MODIS_Cont_Check_logfiles/logfile.txt\n")
		print("The second argument also need to have the name of the .txt file where you wish to log all the missing files.")
		print("You don't need to make a TextFile, just give the name in the second argument and the program will make a txtfle")
		print("with that name in the specified directory.\n")
		
		print("3. The third argument is the year you wish to run the program on.\n")
		
		print("4. The fourth argument is the path to the dateTime file. It should be a txt file that contains all the times in")
		print("the day at an interval of 5 minutes starting from 0000 to 2355. For example, ")
		print("\npath/to/orbFile or /home/user/File_Verification_Scripts/scripts/dateTime.txt\n")

def run(dirpath, logfile, year, datetime):
	Continuity_Check(dirpath, logfile, year, datetime)
	


def Continuity_Check(dirpath, textfile, year, dateTime):
	exist = {}
	orbitFiles = defaultdict(list)
	leap = [2000, 2004, 2008, 2012, 2016, 2020, 2024]

	checklist = []
	numlist = []
	emptydir = []
	hdfempty = []

	#makes a list of all the days in the year
	for i in range(1, 367):
		checklist.append(i)

	#this makes a list of all the dates than need to be checked
	with open(dateTime) as file:
		for line in file:
			numlist.append(line[0:4])

	#this is the text file where all the data is written 
	fileCheck = open(textfile, "w").close()
	fileCheck = open(textfile, "w")

	#changing into the year directory
	dirPath = os.path.join(dirpath, year)
	
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

	#removes the extra day if its not a leap year
	if year not in leap:
		checklist.remove('366')
	
	#writes to the text file if and day is missing from the year
	if missinglist:
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
