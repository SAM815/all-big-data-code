#!/usr/bin/env python

import sys

maxPopulation = {}
maxName = ''

for line in sys.stdin:
    line = line.strip()
    temp, key, val = line.split("\t")
    val = int(val)
    try:
        if val > maxPopulation[maxName]:
            maxPopulation.clear()
            maxName = key
            maxPopulation[maxName] = val
        else:
            continue
    except:
        maxPopulation[key] = val
        maxName = key

for key in maxPopulation.keys():
	print("%s\t%i" % (key, maxPopulation[key]))
