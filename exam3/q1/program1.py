# Write Hadoop/Pyspark MapReduce code to list the average price for all makes. Consider 0 for missing price attributes denoted by “?”.

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext("local","Pyspark Practice Problem")

    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))
    

    lines = lines.map(lambda x: (str(x[2]),(x[25] ,1))).mapValues(lambda x: (0,x[1]) if x[0] == "?" else (float(x[0]),1))
    
    lines = lines.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

    lines = lines.mapValues(lambda x: x[0]/x[1])

    lines.saveAsTextFile("/output/avg")