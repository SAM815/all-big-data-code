# - [ ] c) pySpark code to find the total number of countries have at least one circle in their flag in each landmass

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    #create spark context 
    sc = SparkContext("local","Pyspark exam practice example")
    
    #read data from a text file and split each words into two
    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))
    	# print('%s\t%s' %(line[0],line[18]))

    lines = lines.map(lambda x: ((str(x[1]),(int(x[18]),1)) if (int(x[18]) >= 1) else (str(x[0]),(int(x[18]),0))))
    result = lines.mapValues(lambda x: x[1])
    result = result.reduceByKey(lambda x,y: (x+y))
    result = result.filter(lambda x: x[1] > 0)



    #save the counts to output
    result.saveAsTextFile("/output/c")
                      
