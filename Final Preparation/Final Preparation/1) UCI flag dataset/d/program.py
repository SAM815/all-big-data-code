# - [ ] d) pySpark code to find the average population in each landmass

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    #create spark context 
    sc = SparkContext("local","Pyspark exam practice example")
    
    #read data from a text file and split each words into two
    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))

    # print('%s\t%s' %(line[1],line[4]))

    lines = lines.map(lambda x: (x[1], (int(x[4]),1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).mapValues(lambda x: x[0]/x[1])

    lines.saveAsTextFile("/output/avg")
