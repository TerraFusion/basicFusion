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
import re
import shutil


dirpath = '/projects/TDataFus/BFData/MISR/MI1B2E.003'

def main():
	if not len(sys.argv) > 1:
		usage()

	elif sys.argv[1] == '-a':
		arguments()

	elif sys.argv[1] == '-r':
		if len(sys.argv) != 4:
			arguments()
		else:
			dirpath = sys.argv[2]
			logfile = sys.argv[3]
			move(dirpath, logfile)
	else:
		usage()

	# dirpath = '/projects/TDataFus/gyzhao/TF/data/MISR/MI1B2E.003'
	#orbFile = ''

def usage():
	print("\n\nUSAGE:  python MISR_move.py [-h | -a | -r]\n")
	print("Options:\n")
	print("-h     									More information about the different functions and how they work\n")
	print("-a           								More information about the arguments and the format to run the program\n")
	print("-r [dirpath] [logfile]			  				Run the program\n")

def arguments():
	print("\nARGUMENTS TO RUN PROGRAM:\n\n python MISR_move.py -r [absolute/path/to/dir] [filepath/to/logfile]\n")
	print("For more information on each argument, type i.")
	info = str(raw_input())
	
	if info == 'i' or info == 'I':
		print("MORE INFORMATION: \n\nFor this program to run properly, you need 2 arguments.\n")
		print("1. The first is the directory path. For example, it should look something like:")
		print("\npath/to/dir or /projects/TDataFus/gyzhao/TF/data/MISR/MI1B2E.003\n")
		print("In order for the program to run smoothly, you need to provide the absolute path and make sure that you")
		print("provide the dirpath only uptil just before the dates.\n")
		
		print("2. The second argument is the file path to the logfile which contains the names of the missing files.")
		print("For example, it should look something like,")
		print("\n/path/to/logfile/logfile.txt or /home/user/scratch/BasicFusion/MISR_logfiles/logfile.txt\n")
		print("The second argument also need to have the name of the .txt file.")

	else:
		return


def move(dirpath, logfile):

	fileCheck = open("logfile", "r")

	#for each missing file name in logfile
	for line in fileCheck:
		fname = line.split(" ", 1)[0]
		#each line contains the wrong date and the date it should be in
		wrongDate = line.split(" ", 4)[3]
		rightDate = line.split(" ", 7)[7]
		dateList = convert2Format(wrongDate, rightDate)

		#moves the files from the wrong date to correct date
		fromPath = os.path.join(dirpath, dateList[0])
		toPath = os.path.join(dirpath, dateList[1])
		os.rename(os.path.join(fromPath, fname), os.path.join(toPath, fname))

#the function just converts the dates to the correct format
def convert2Format(date1, date2):
	dateList = []
	date1C = date1[0:4] + '.' + date1[5:7] + '.' + date1[8:10]
	date2C = date2[0:4] + '.' + date2[5:7] + '.' + date2[8:10]
	dateList.append(date1C)
	dateList.append(date2C)

	return dateList


move()
