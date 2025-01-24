#!/usr/bin/env python

import sys


for line in sys.stdin:
	val = line.strip()
	columns = val.split(',')
	key = 1
	name = columns[0]
	population = columns[4]

	print('%s\t%s\t%s' % (key, name, population))

------------------------------------------------------------
Professor's code:

#!/usr/bin/env python

import sys

fixedkey = 1

for line in sys.stdin:
	line = line.strip()
	line = line.split(",")
	print("%s\t%s" % (fixedkey, line[0], line[4] )

//Reducer.py

#!/usr/bin/env python

import sys

maxpopulation = 0
maxcountry = None


for line in sys.stdin:
	line = line.strip()
	fixedkey, name, population= line.split("\")
	population = float(population)

	if population > maxpopulation:
		maxpopulation = population
		maxcountry = name
	else:
		continue

print('%s\t%s' % (maxcountry, maxpopulation))

	
---------------------------------------------------------


