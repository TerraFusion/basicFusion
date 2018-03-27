# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import re
import numpy

orbits = {}
sizes = []
size_total = 0
with open('./all_bf.txt', 'r') as f:
    for line in f:
        m = re.search( '_O[0-9]+_', line )

        if m:
            orbit = m.group(0)

            orbit = orbit.replace('_O', '').replace('_', '')
            size = int(line.split()[6])
            file = line.split()[-1]
            orbits[orbit] = { 'file' : file, 'size' : size }
            size_total += int(size)
            sizes.append( size / 1000000 )

arr = numpy.array( sizes )
print('mean: {}'.format( numpy.mean(arr) ))
print('std: {}'.format( numpy.std(arr)))

plt.hist( sizes, bins='doane')
plt.title('Basic Fusion file sizes')
plt.xlabel("Size (MB)")
plt.ylabel("Count")
plt.text( 50000, 12000, u'μ: {} \nmed: {} \nσ: {}'.format( numpy.mean(arr), numpy.median(arr), numpy.std(arr)), bbox=dict( facecolor='white', alpha=0.5) )
plt.savefig( './bf_size.png', bbox_inches='tight' )
plt.close()
