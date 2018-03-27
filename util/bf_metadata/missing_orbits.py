import re
orbits = {}
zeros = []
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
            if size == 0:
                zeros.append( int( orbit ) )

total_size = 0

for orbit in range(1000, 85303):
    if str(orbit) in orbits:
        total_size += orbits[str(orbit)]['size']

print( zeros )
