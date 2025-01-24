# Write Hadoop/Pyspark MapReduce code to find the country which has the highest population for each landass.

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    #create spark context 
    sc = SparkContext("local","Pyspark exam  example")
    
    #read data from a text file and split each words into two
    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))

    

    lines = lines.map(lambda x: (x[1], int(x[4])))

    lines = lines.reduceByKey(lambda x,y: x if x > y else y)

    lines.saveAsTextFile("/output")
