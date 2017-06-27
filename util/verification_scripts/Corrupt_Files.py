"""
@author : Shashank Bansal
@email : sbansal6@illinois.edu

Date : June 22, 2017

"""

import subprocess 
import os
import thread
import multiprocessing
import sys



def main():
	#dirpath = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD02HKM/2013/258/MOD02HKM.A2013258.0855.006.2014223185453.hdf'
	#dirpath = '/projects/TDataFus/gyzhao/TF/data/MODIS/MOD02HKM/2013/338/MOD02HKM.A2013338.2350.006.2014223184417.hdf'
	arglist = sys.argv[1:]
	dirpath = str(arglist[0])
	logfile = str(arglist[1])
	month = arglist[2]
	#dirpath = "/projects/TDataFus/gyzhao/TF/data/MODIS/MOD02HKM/2013/"
	#logfile = "MODIS_Corrupt_Check_MOD021KM_2013.txt"

	check_Corrupt(dirpath, logfile, month)

def get_range(month, leap):

	if month == 1: 
		return [1, 31]
	elif month == 2:
		return [32,60]
	elif month == 3:
		return [61,92]
	elif month == 4:
		return [93,123]
	elif month == 5:
		return [124,155]
	elif month == 6:
		return [156,186]
	elif month == 7:
		return [187,218]
	elif month == 8:
		return [219,250]
	elif month == 9:
		return [251,281]	
	elif month == 10:
		return [282,311]
	elif month == 11:
		return [312,342]
	elif month == 12 and leap == False:
		return [342,365]
	elif month == 12 and leap == True:
		return [342, 366]


def check_Corrupt(dirPath, logfile, month):

	fileCheck = open(logfile, "w").close()
	fileCheck = open(logfile, "w")
	
	emptydir = []
	zero_size = {}
	Range = []
	Range = get_range(int(month), False)
	daylist = []

	for i in range(Range[0], Range[1]+1):
		daylist.append(i)
	count = 0
	#entering the year to be checked (day number)
	for fname in os.listdir(dirPath):
		if(os.listdir(os.path.join(dirPath, fname)) == []):
			emptydir.append(fname)


		else:
			#this makes sure that we do not enter an empty directory
			if fname not in emptydir and int(fname) in daylist:
				print(fname)
				inPath = os.path.join(dirPath, fname)

				for filename in os.listdir(inPath):
					#print(filename)
					hdfinfo = os.stat(os.path.join(inPath, filename))

					if(hdfinfo.st_size == 0):
						zero_size[filename] = True

					else:
						strin = subprocess.Popen(["hdp", "dumpsds", "-bsglh", os.path.join(inPath, filename)], stdout=subprocess.PIPE, stderr = subprocess.PIPE)
						retval = strin.communicate()
						retval1 = strin.returncode
						#print(retval1)
						#print(len(retval))
						if int(retval1) == 1:
							fileCheck.write(os.path.join(inPath, filename) + '\n')

	if zero_size:
		fileCheck.write("These files are empty: \n")
		for i in zero_size:
			fileCheck.write(i + '\n')



main()

