# a) MapReduce code to find the total number of countries have at least one circle in their flag in each landmass
import sys

count= 0

for line in sys.stdin:
        line= line.strip()
        key, val= line.split('\t')

        try:
                val= int(val)
        except ValueError:
                continue
        
        if val>0:
                count= count+1

print("Total number of countries have at least one circle in their flag in each landmass: %d" %(count))
