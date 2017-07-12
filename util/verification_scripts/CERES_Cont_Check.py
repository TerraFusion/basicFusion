"""
@author : Shashank Bansal
@email : sbansal6@illinois.edu

"""

import os
from collections import defaultdict
from itertools import cycle
import sys

"""#----INFORMATION ON HOW THIS FUNCTION WORKS----#

#dirpath is the path to the inputFiles folder
#EXAMPLE: /home/path/to/directory/inputFiles

#logfile is the absolute path with filename to the txt where the missing files need to be logged
#EXAMPLE: /path/to/log/directory/logfile.txt 

"""


def main():

	if not len(sys.argv) > 1:
		usage()

	elif sys.argv[1] == '-h' or sys.argv[1] == '-help' :
		help()

	elif sys.argv[1] == '-a':
		arguments()

	elif sys.argv[1] == '-r':
		run()


	
	#dirpath = '/home/sbansal6/inputFiles/2013'
	#logfile = '/home/sbansal6/logfiles/CERES_Cont_Check.txt'
	
	# dirpath = '/home/sbansal6/File_Verification_Scripts/inputFiles'
	# logfile = '/home/sbansal6/File_Verification_Scripts/testlog.txt'

	#dirpath = '/projects/sciteam/jq0/TerraFusion/inputFiles'
	#logfile = '/u/sciteam/sbansal6/scratch/logfiles/CERES_Cont_Check.txt'
	
	#Continuity_Check(dirpath, logfile)

def usage():
	print("\n\n#------------------========================USAGE=======================-------------------#\n")
	print("Options:\n")
	print("-h -help     More information about the different functions and how they work\n")
	print("-a           More information about the arguments and the format for the program to run\n")
	print("-r           Run the program\n")


def help():
	print("\nThere are 2 main functions in this program.")
	print("1. Continuity_Check(dirpath, logfile)")
	print("2. Existence_Check\n")
	print("1. Continuity_Check(dirpath, logfile): \nThis function is used to check if their is any discontinuity/missing")
	print("FM1 or FM2 hour in the inputFile.txt. For example, lets say that the following is an example inputfile.txt\n")
	print("/path/to/dir/CER_SSF_Terra-FM1-MODIS_Edition4A_400403.2013101314")
	print("/path/to/dir/CER_SSF_Terra-FM1-MODIS_Edition4A_400403.2013101316")
	print("/path/to/dir/CER_SSF_Terra-FM1-MODIS_Edition4A_400403.2013101317\n")
	print("Here, \n/path/to/dir/CER_SSF_Terra-FM1-MODIS_Edition4A_400403.2013101315 \nis missing which is the hour 15 of the day.")
	print("Continuity_Check makes a list of all such missing files.\n")
	print("2. Existence_Check: This function checks if the missing files from Continuity_Check actually exist in the CERES database.")
	print("If the file does not exist even in the database then it is removed from the missing file list.\n")

def arguments():
	print("\nFor this program to run properly, you just need 2 main arguments. The first is the directory path for the inputFiles.")
	print("For example, it should look something like:")
	print("\npath/to/dir or /home/user/scratch/BasicFusion/inputFiles\n")
	print("In order for the program to run smoothly, you need to provide the absolute path to the inputFiles.\n")
	print("The second argument is the file path to the logfile where you want to write the names of the missing files.")
	print("For example, it should look something like,")
	print("\n/path/to/logfile/logfile.txt or /home/user/scratch/BasicFusion/logfiles/logfile.txt\n")
	print("The second argument also need to have the name of the .txt file where you wish to log all the missing files.")
	print("You don't need to make a TEXTFile just give the name in the second argument and the program will make a txt")
	print("with that name in the specified directory.\n")


def run():
	print('Please provide the absolute path to the directory where the input files are.')
	print('EXAMPLE: /home/path/to/directory/inputFiles')
	dirpath = str(raw_input())
	print('\n')
	print('Please provide the filepath to the logfile.')
	print('EXAMPLE: /path/to/log/directory/logfile.txt')
	logfile = str(raw_input())

	Continuity_Check(dirpath, logfile)

#checks if the file missing from the input file actually exists
#in the database
def Existence_Check(checkdirpath, filename):
	if os.path.exists(checkdirpath):
		for hdffile in os.listdir(checkdirpath):
			if(filename == hdffile):
				return True

		return False

	return False


def Continuity_Check(dirpath, logfile):

	contCheck = open(logfile, "w").close()
	contCheck = open(logfile, "w")

	if os.path.exists(dirpath):
		for txtfile in os.listdir(dirpath):
			strlistFM1 = []
			strlistFM2 = []
			missingFM1 = []
			missingFM2 = []
			d = defaultdict(list)
			stats = os.stat(os.path.join(dirpath, txtfile))
			if txtfile.endswith('.txt') and stats.st_size != 0:
				hourFM1 = []
				hourFM2 = []
				with open(os.path.join(dirpath, txtfile)) as t:
					for line in t:
						#the following makes a list of all the filenames in the inputfile 
						#and a list of corresponding hours in the 
						#inputfile for both FM1 and FM2
						
						if "CERES" and "FM1" in line:
							strlistFM1.append(line)
							hourFM1.append(int(line[len(line)-3:len(line)]))

						if "CERES" and "FM2" in line:
							strlistFM2.append(line)
							hourFM2.append(int(line[len(line)-3:len(line)]))

				#check for all FM1 files here
				for i in range(len(hourFM1)-1):
					if(hourFM1[i] != 23):
						if hourFM1[i] + 1 != hourFM1[i+1]:
							#makes a list of all the missing FM1 files from the inputfiles
							for j in range(hourFM1[i]+1, hourFM1[i+1]):
								strj = str(j)
								#makes sure that the file is in right format
								if(len(strj) == 1):
									missingFM1.append(strlistFM1[i][0:len(strlistFM1[i])-3] + '0' + strj)
								elif(len(strj) == 2):
									missingFM1.append(strlistFM1[i][0:len(strlistFM1[i])-3] + strj)
					
					#need to loop around back to 00 for the special case of 23
					elif(hourFM1[i] == 23):
						if hourFM1[i+1] != 0:
							#for the special case of 23 where it should cycle around back to 00
							for j in range(0, hourFM1[i+1]):
								strj = str(j)
								#makes sure that the file is in right format
								if(len(strj) == 1):
									missingFM1.append(strlistFM1[i][0:len(strlistFM1[i])-3] + '0' + strj)
								elif(len(strj) == 2):
									missingFM1.append(strlistFM1[i][0:len(strlistFM1[i])-3] + strj)

				#check for all FM2 files here
				for i in range(len(hourFM2)-1):

					if(hourFM2[i] != 23):
						if hourFM2[i] + 1 != hourFM2[i+1]:
							#makes a list of all the missing FM2 files from the inputfiles
							for j in range(hourFM2[i]+1, hourFM2[i+1]):
								strj = str(j)
								#makes sure that the file is in right format
								if(len(strj) == 1):
									missingFM2.append(strlistFM2[i][0:len(strlistFM2[i])-3] + '0' + strj)
								elif(len(strj) == 2):
									missingFM2.append(strlistFM2[i][0:len(strlistFM2[i])-3] + strj)
					
					#need to loop around back to 00 for the special case of 23
					elif(hourFM2[i] == 23):
						if hourFM2[i+1] != 0:
							#for the special case of 23 where it should cycle around back to 00
							for j in range(0, hourFM2[i+1]):
								strj = str(j)
								#makes sure that the file is in right format
								if(len(strj) == 1):
									missingFM1.append(strlistFM2[i][0:len(strlistFM2[i])-3] + '0' + strj)
								elif(len(strj) == 2):
									missingFM1.append(strlistFM2[i][0:len(strlistFM2[i])-3] + strj)


			#this makes sure that the file exists in the database for both FM1 and FM2
			#if the file doesnot even exist in the database then it will not be logged as missing
			if missingFM1:
				for i in missingFM1:
					templst = i.rsplit('/', 1)
					existence = Existence_Check(templst[0], templst[1])
					if existence == False:
						missingFM1.remove(i)


			if missingFM2:
				for i in missingFM2:
					templst = i.rsplit('/', 1)
					existence = Existence_Check(templst[0], templst[1])
					if existence == False:
						missingFM2.remove(i)

			#finally missingFM1 and missingFM2 are the lists all the missing files from input
			#which exist in the database too
			if missingFM1 or missingFM2:
				contCheck.write(os.path.join(dirpath,txtfile) + '\n')
				if missingFM1:
					for i in missingFM1:
						contCheck.write(i + '\n')

				if missingFM2:
					for i in missingFM2:
						contCheck.write(i + '\n')
				
				contCheck.write('\n')

main()
