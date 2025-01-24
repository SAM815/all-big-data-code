# Q4. Consider the “Flag” data set in Q3. Write Hadoop/Pyspark to find the highest spoken language by countries in each landmass.

import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	sc = SparkContext("local", "Pyspark exam Problem")
	lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))
	
	result = lines.map( lambda x: ((int(x[1]),int(x[5])), (int(x[4])) ))
	result = result.reduceByKey(lambda x,y: x + y)
	result = result.map(lambda x: (x[0][0], (x[0][1], int(x[1]) )))
	result = result.reduceByKey(lambda x,y: ( x if x[1] > y[1] else y ))
	
	
	def convert(x):
		if x == 1:
			x = "English"
		elif x == 2:
			x = "Spanish"
		elif x == 3:
			x = "French"
		elif x == 4:
			x = "German"
		elif x == 5:
			x = "Slavic"
		elif x == 6:
			x = "Other Indo-European"
		elif x == 7:
			x = "Chinese"
		elif x == 8:
			x = "Arabic"	
		elif x == 9:
			x = "Japanese/Turkish/Finnish/Magyar"
		elif x == 10:
			x = "Others"

		return x

	result = result.mapValues(lambda x: (convert(x[0]),x[1]) )
	
	result.saveAsTextFile("/output")
