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
#EXAMPLE: /path/to/log/directory/logfile.txt """

def main():
	
	#dirpath = '/home/sbansal6/inputFiles/2013'
	#logfile = '/home/sbansal6/logfiles/CERES_Cont_Check.txt'
	
	dirpath = sys.argv[0]
	logfile = sys.argv[1]
	#dirpath = '/projects/sciteam/jq0/TerraFusion/inputFiles'
	#logfile = '/u/sciteam/sbansal6/scratch/logfiles/CERES_Cont_Check.txt'
	
	Continuity_Check(dirpath, logfile)


def Existence_Check(checkdirpath, filename):

	if os.path.exists(dirpath):
		for hdffile in os.listdir(dirpath):
			if(filename == hdffile):
				return True

		return False


def Continuity_Check(dirpath, logfile, checkdirpath):

	contCheck = open(logfile, "w").close()
	contCheck = open(logfile, "w")

	if os.path.exists(dirpath):
		for txtfile in os.listdir(dirpath):
			#print txtfile
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

				#print(txtfile)
				#print(hourFM1)
				#print(hourFM2)
				#contCheck.write(txtfile + '\n')
				#check for all FM1 files here
				for i in range(len(hourFM1)-1):
					if(hourFM1[i] != 23):
						if hourFM1[i] + 1 != hourFM1[i+1]:
							#contCheck.write('\n' + txtfile + '\n')
							#makes a list of all the missing FM1 files from the inputfiles
							for j in range(hourFM1[i]+1, hourFM1[i+1]):
								strj = str(j)
								#makes sure that the file is in right format
								if(len(strj) == 1):
									missingFM1.append(strlistFM1[i][0:len(strlistFM1[i])-2] + '0' + strj)
								elif(len(strj) == 2):
									missingFM1.append(strlistFM1[i][0:len(strlistFM1[i])-2] + strj)
					
					#need to loop around back to 00 for the special case of 23
					elif(hourFM1[i] == 23):
						if hourFM1[i+1] != 0:
							#contCheck.write('\n' + txtfile + '\n')
							#for the special case of 23 where it should cycle around back to 00
							for j in range(0, hourFM1[i+1]):
								strj = str(j)
								#makes sure that the file is in right format
								if(len(strj) == 1):
									missingFM1.append(strlistFM1[i][0:len(strlistFM1[i])-2] + '0' + strj)
								elif(len(strj) == 2):
									missingFM1.append(strlistFM1[i][0:len(strlistFM1[i])-2] + strj)

				#check for all FM2 files here
				for i in range(len(hourFM2)-1):

					if(hourFM2[i] != 23):
						if hourFM2[i] + 1 != hourFM2[i+1]:
							#contCheck.write('\n' + txtfile + '\n')
							#makes a list of all the missing FM2 files from the inputfiles
							for j in range(hourFM2[i]+1, hourFM2[i+1]):
								strj = str(j)
								#makes sure that the file is in right format
								if(len(strj) == 1):
									missingFM2.append(strlistFM2[i][0:len(strlistFM2[i])-2] + '0' + strj)
								elif(len(strj) == 2):
									missingFM2.append(strlistFM2[i][0:len(strlistFM2[i])-2] + strj)
					
					#need to loop around back to 00 for the special case of 23
					elif(hourFM2[i] == 23):
						if hourFM2[i+1] != 0:
							#contCheck.write('\n' + txtfile + '\n')
							#for the special case of 23 where it should cycle around back to 00
							for j in range(0, hourFM2[i+1]):
								strj = str(j)
								#makes sure that the file is in right format
								if(len(strj) == 1):
									missingFM1.append(strlistFM2[i][0:len(strlistFM2[i])-2] + '0' + strj)
								elif(len(strj) == 2):
									missingFM1.append(strlistFM2[i][0:len(strlistFM2[i])-2] + strj)


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

			if missingFM1 or missingFM2:
				#contCheck.write('These are the FM1 and FM2 missing/non-continuous files: \n\n')
				contCheck.write(os.path.join(dirpath,txtfile) + '\n')
				
				if missingFM1:
					for i in missingFM1:
						contCheck.write(i + '\n')

				if missingFM2:
					for i in missingFM2:
						contCheck.write(i + 'n')
				
				contCheck.write('\n')

main()