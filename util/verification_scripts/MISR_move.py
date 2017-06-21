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

def move():

	#fileCheck = open("MISR_date_Check.txt", "r").close()
	fileCheck = open("MISR_date_Check.txt", "r")

	for line in fileCheck:
		fname = line.split(" ", 1)[0]
		wrongDate = line.split(" ", 4)[3]
		rightDate = line.split(" ", 7)[7]
		dateList = convert2Format(wrongDate, rightDate)
		fromPath = os.path.join(dirpath, dateList[0])
		toPath = os.path.join(dirpath, dateList[1])
		os.rename(os.path.join(fromPath, fname), os.path.join(toPath, fname))

def convert2Format(date1, date2):
	dateList = []

	date1C = date1[0:4] + '.' + date1[5:7] + '.' + date1[8:10]
	date2C = date2[0:4] + '.' + date2[5:7] + '.' + date2[8:10]
	dateList.append(date1C)
	dateList.append(date2C)

	return dateList


move()