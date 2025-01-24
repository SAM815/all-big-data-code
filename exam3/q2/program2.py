# Q2. Consider the “Automobile” data set in Q1. Write Hadoop/Pyspark MapReduce code to list the maximum price for all makes. Consider 0 for missing price attributes denoted by “?”.

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext("local","Pyspark exam Problem")

    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))
    
    lines = lines.map(lambda x: (str(x[2]), x[25])).mapValues(lambda x: (0 if x == "?" else float(x)))

    lines = lines.reduceByKey(lambda x,y: x if x > y else y)

    lines.saveAsTextFile("/output/max")

