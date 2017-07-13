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


#python Corrupt_Files_all.py -r /projects/TDataFus/gyzhao/TF/data/MODIS/MOD021KM /home/sbansal6/File_Verification_Scripts/MODIS_Corrupt/MODIS_Corrupt_Check_MOD021KM_2009_1.txt 2009 1
def main():

	if not len(sys.argv) > 1:
		usage()

	elif sys.argv[1] == '-h' or sys.argv[1] == '-help':
		help()

	elif sys.argv[1] == '-a':
		arguments()

	elif sys.argv[1] == '-r':
		if len(sys.argv) <= 6:
			arguments()
		else:
			instr = sys.argv[2]
			dirpath = sys.argv[3]
			logfile = sys.argv[4]
			year = sys.argv[5]
			
			if instr == 'MOPITT' or instr == 'mopitt':
				MOPITT_Corrupt(dirpath, logfile, year)
	
			
			elif instr == 'MISR' or instr == 'misr':
				month = sys.argv[6]
				MISR_Corrupt(dirpath, logfile, year, month)
	
			
			elif instr == 'CERES' or instr == 'ceres':
				CERES_Corrupt(dirpath, logfile, year)

			
			elif instr == 'ASTER' or instr == 'aster':
				month = sys.argv[6]
				ASTER_Corrupt(dirpath, logfile, year, month)

			
			elif instr == 'MODIS' or instr == 'modis':
				month = sys.argv[6]
				MODIS_Corrupt(dirpath, logfile, year, month)
			
			else:
				arguments()



def usage():
	print("\n\nUSAGE:  python Corrupt_Files_all.py [-h | -a | -r]\n")
	print("Options:\n")
	print("-h     										More information about the different functions and how they work\n")
	print("-a           									More information about the arguments and the format to run the program\n")
	print("-r [instr] [dirpath] [logfile] [YEAR] [monthR/month](optional)  		Run the program\n")
	#print("-m           						Run the program on multiple years at once.")


def help():
	print("\nDESCRIPTION:\n\nThere are 5 different functions in this program- \n")
	print("1. MODIS_Corrupt(dirPath, logfile, YEAR, monthR)")
	print("2. MOPITT_Corrupt(dirPath, logfile, YEAR)")
	print("3. MISR_Corrupt(dirPath, logfile, YEAR, month)")
	print("4. CERES_Corrupt(dirPath, logfile, YEAR)")
	print("5. ASTER_Corrupt(dirPath, logfile, YEAR, month)\n")

	print("To check if a file is corrupt or not, we run 'hdp dumpsds -h [filename]' and check for the value")
	print("that is returned. A return value of other than 0 means it is a corrupt file.\n")
	print("NOTE: In case of MOPITT files, we use 'ncdump -h' instead because of the different format of the files.\n")


def arguments():
	print("\nARGUMENTS TO RUN PROGRAM:\n\n python MISR_all_years.py -r [instr] [absolute/path/to/dir] [absolute/filepath/to/logfile] [YEAR] [month or monthR](optional)\n")
	print("For more information on each argument, type i.")
	info = str(raw_input())
	
	if info == 'i' or info == 'I':
		print("MORE INFORMATION: \n\nThe arguments depend on the instrument you choose. Each instrument has their own number and type")
		print("of arguments.")
		print("1. The first argument is [instr] which takes the type of instrument. Following are the choices you have for [instr].\n")
		print("MODIS")
		print("MOPITT")
		print("MISR")
		print("CERES")
		print("ASTER\n")

		print("For all the instruments, you need to give [dirPath] [logfile] [YEAR] .\n")
		print("[dirPath] is the path to the directory where the data is stored. For example,")
		print("\npath/to/dir or /projects/TDataFus/gyzhao/TF/data/MISR/MI1B2E.003\n")
		print("In order for the program to run smoothly, you need to provide the absolute path and make sure that you")
		print("provide the dirpath only uptil just before the dates.\n")
		
		print("2. The next argument is the file path to the logfile where you wish to write the names of the missing files.")
		print("For example, it should look something like,")
		print("\n/path/to/logfile/logfile.txt or /home/user/scratch/BasicFusion/MISR_logfiles/logfile.txt\n")
		print("[logfile] also need to have the name of the .txt file where you wish to log/write all the missing files.")
		print("You don't need to actually make a textFile, just give the name in the second argument and the program will make a txtfle")
		print("with that name in the specified directory.\n")
		
		print("3. The next argument is [YEAR] which is the year you wish to run the program on.\n")
		
		print("4. The last argument is either [monthR] (for MODIS) or [month] (MISR and ASTER). [monthR] for MODIS ranges in [1 15]")
		print("and [month] for MISR or ASTER ranges in [1 12].")

		print("\nNOTE: monthR ranges in [1 15] because MODIS has a lot of files and this range helps to further parallelize and")
		print("thus improve the speed of the program. Each number for [monthR] corresponds to a day range between [1 366]\n")


	else:
		return


#this returns the range of days for the value of monthR in MODIS
def get_range(month):

	if month == 1: 
		return [1, 23]
	elif month == 2:
		return [23,46]
	elif month == 3:
		return [46,69]
	elif month == 4:
		return [69,92]
	elif month == 5:
		return [92,115]
	elif month == 6:
		return [115,138]
	elif month == 7:
		return [138,161]
	elif month == 8:
		return [161,184]
	elif month == 9:
		return [184,207]	
	elif month == 10:
		return [207,230]
	elif month == 11:
		return [230,267]
	elif month == 12:
		return [267,304]
	elif month == 13:
		return [304, 327]
	elif month == 14:
		return [327, 350]
	elif month == 15:
		return [350, 367]


def MODIS_Corrupt(dirpath, logfile, YEAR, monthR):

	fileCheck = open(logfile, "w").close()
	fileCheck = open(logfile, "w")
	
	emptydir = []
	zero_size = {}
	Range = []
	Range = get_range(int(monthR))
	daylist = []


	for i in range(Range[0], Range[1]):
		daylist.append(i)
		
	dirPath = os.path.join(dirpath, YEAR)
	#entering the year to be checked (day number)
	for fname in os.listdir(dirPath):
		if(os.listdir(os.path.join(dirPath, fname)) == []):
			emptydir.append(fname)


		else:
			#this makes sure that we do not enter an empty directory
			if fname not in emptydir and int(fname) in daylist:
				#print(fname)
				inPath = os.path.join(dirPath, fname)

				for filename in os.listdir(inPath):
					#print(filename)
					hdfinfo = os.stat(os.path.join(inPath, filename))

					if(hdfinfo.st_size == 0):
						zero_size[filename] = True

					else:
						strin = subprocess.Popen(["hdp", "dumpsds", "-h", os.path.join(inPath, filename)], stdout=subprocess.PIPE, stderr = subprocess.PIPE)
						retval = strin.communicate()
						retval1 = strin.returncode
						if int(retval1) != 0:
							fileCheck.write(os.path.join(inPath, filename) + '\n')

	if zero_size:
		fileCheck.write("These files are empty: \n")
		for i in zero_size:
			fileCheck.write(i + '\n')

def MOPITT_Corrupt(dirpath, logfile, YEAR):

	zero_size = {}

	year = str(YEAR)

	dateStr = []

	fileCheck = open(logfile, "w").close()
	fileCheck = open(logfile, "w")
	
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

		if date in dateStr:
			dateStr.remove(date)
			inPath = os.path.join(dirpath, date)

			#print(date)
			for fname in os.listdir(inPath):
				if fname.endswith('.he5'):

					#print(fname)


					hdfinfo = os.stat(os.path.join(inPath, fname))

					if(hdfinfo.st_size == 0):
						zero_size[fname] = True

					else:
						strin = subprocess.Popen(["ncdump", "-h", os.path.join(inPath, fname)], stdout=subprocess.PIPE, stderr = subprocess.PIPE)
						retval = strin.communicate()
						retval1 = strin.returncode
						#print(retval1)
						if int(retval1) != 0:
							fileCheck.write(os.path.join(inPath, fname) + '\n')

def MISR_Corrupt(dirpath, logfile, YEAR, month):

	year = str(YEAR)

	dateStr = []

	fileCheck = open(logfile, "w").close()
	fileCheck = open(logfile, "w")
	
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

		mon = date.split('.')[1]

		if date in dateStr and int(mon) == int(month):
			dateStr.remove(date)
			inPath = os.path.join(dirpath, date)

			print(date)
			for fname in os.listdir(inPath):
				if fname.endswith('.hdf'):

					#print(fname)


					hdfinfo = os.stat(os.path.join(inPath, fname))

					if(hdfinfo.st_size == 0):
						zero_size[fname] = True

					else:
						strin = subprocess.Popen(["hdp", "dumpsds", "-h", os.path.join(inPath, fname)], stdout=subprocess.PIPE, stderr = subprocess.PIPE)
						retval = strin.communicate()
						retval1 = strin.returncode
						#print(retval1)
						if int(retval1) != 0:
							fileCheck.write(os.path.join(inPath, fname) + '\n')


def CERES_Corrupt(dirpath, logfile, YEAR):
	year = str(YEAR)


	fileCheck = open(logfile, "w").close()
	fileCheck = open(logfile, "w")

	yearpath = os.path.join(dirpath, year)

	for cerfile in os.listdir(yearpath):
		if not cerfile.endswith('.met'):
			#print(cerfile)
			strin = subprocess.Popen(["hdp", "dumpsds", "-h", os.path.join(yearpath, cerfile)], stdout=subprocess.PIPE, stderr = subprocess.PIPE)
			retval = strin.communicate()
			retval1 = strin.returncode
			#print retval1
			if int(retval1) != 0:
				fileCheck.write(os.path.join(yearpath, cerfile) + '\n')


def ASTER_Corrupt(dirpath, logfile, YEAR, month):
	year = str(YEAR)

	dateStr = []

	fileCheck = open(logfile, "w").close()
	fileCheck = open(logfile, "w")
	
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

		mon = date.split('.')[1]

		if date in dateStr and int(mon) == int(month):
			dateStr.remove(date)
			inPath = os.path.join(dirpath, date)

			#print(date)
			for fname in os.listdir(inPath):
				if fname.endswith('.hdf'):

					#print(fname)


					hdfinfo = os.stat(os.path.join(inPath, fname))

					if(hdfinfo.st_size == 0):
						zero_size[fname] = True

					else:
						strin = subprocess.Popen(["hdp", "dumpsds", "-h", os.path.join(inPath, fname)], stdout=subprocess.PIPE, stderr = subprocess.PIPE)
						retval = strin.communicate()
						retval1 = strin.returncode
						#print(retval1)
						if int(retval1) != 0:
							fileCheck.write(os.path.join(inPath, fname) + '\n')



main()