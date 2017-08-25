"""
@author : Yat Long Lo
@email : yllo2@illinois.edu

PROGRAM DESCRIPTION:

    This script generates all the paths to the input files based on the path given
    
"""
import os
import sys

def main():
    ifile = open("output_files_paths.txt", "w+")
    path = "/scratch/sciteam/clipp/BFoutputUncompressed"
    files = os.listdir(path)
    paths_list = []
    for f in files:
        if("TERRA" in f):
            paths_list.append(path + "/" + f)
    paths_list = sorted(paths_list)
    for p in paths_list:
        ifile.write("%s\n" % p)
    ifile.close()
    
main()