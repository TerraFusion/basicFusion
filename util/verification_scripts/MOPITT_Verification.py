# -*- coding: utf-8 -*-
"""
Spyder Editor

@author : Shashank Bansal
@email : sbansal6@illinois.edu
"""
from os import path
import os

dirpath = '/projects/TDataFus/gyzhao/TF/data/MOPITT/MOP01.005'

exist = {}

for m in range(1, 13):
    for d in range(1, 31):
        if(m < 10 and d < 10):
            dateStr = '2013' + '.0' + str(m) + '.0' + str(d)
        elif(m<10 and d >= 10):
            dateStr = '2013' + '.0' + str(m) + '.' + str(d)
        elif(m>=10 and d < 10):
            dateStr = '2013' + '.' + str(m) + '.0' + str(d)
        else:
            dateStr = '2013' + '.' + str(m) + '.' + str(d)
        
        #print dateStr
        inPath = os.path.join(dirpath, dateStr)
        if not path.exists(inPath):
            exist[ 'MOP01.005' + '/' + dateStr] = 'File Doesnot Exist'
        else:
            for fname in os.listdir(inPath):
                print fname
                if fname.endswith('.hdf'):
                    statinfo = os.stat(os.path.join(inPath, fname))
                    if statinfo.st_size == 0:
                        exist['MOP01.005' + '/' + dateStr] = 'Exists but Empty'
                    else:
                        exist['MOP01.005' + '/' + dateStr] = 'Exists and not Empty'


#special cases for the months with 31st    
if not path.exists(os.path.join(dirpath, '2013.01.31')):
    exist[ 'MOP01.005' + '/' + '2013.01.31'] = 'File Doesnot Exist'

if not path.exists(os.path.join(dirpath, '2013.03.31')):
    exist[ 'MOP01.005' + '/' + '2013.03.31'] = 'File Doesnot Exist'

if not path.exists(os.path.join(dirpath, '2013.05.31')):
    exist[ 'MOP01.005' + '/' + '2013.05.31'] = 'File Doesnot Exist'

if not path.exists(os.path.join(dirpath, '2013.07.31')):
    exist[ 'MOP01.005' + '/' + '2013.07.31'] = 'File Doesnot Exist'

if not path.exists(os.path.join(dirpath, '2013.08.31')):
    exist[ 'MOP01.005' + '/' + '2013.08.31'] = 'File Doesnot Exist'

if not path.exists(os.path.join(dirpath, '2013.10.31')):
    exist[ 'MOP01.005' + '/' + '2013.10.31'] = 'File Doesnot Exist'

if not path.exists(os.path.join(dirpath, '2013.12.31')):
    exist[ 'MOP01.005' + '/' + '2013.12.31'] = 'File Doesnot Exist'

#deleting extra days from the month of Feb
del exist['MOP01.005' + '/' + '2013.02.29']
del exist['MOP01.005' + '/' + '2013.02.30']

for i in exist:
    if exist[i] == 'File Doesnot Exist':
        print i, exist[i]
                                
                    
                    
    

