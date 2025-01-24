#c) pySpark code to find the total number of countries have at least one circle in their flag in each landmass

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext("local","Pyspark Practice Problem")

    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))

    lines = lines.map(lambda x: (str(x[1]),(str(x[0]),int(x[18]),1)) if (int(x[18]) >= 1) else (str(x[1]),(str(x[0]),int(x[18]),0)))

    result = lines.mapValues(lambda x: x[2])
    result = result.reduceByKey(lambda x,y: (x+y))
    

    result.saveAsTextFile("/output")
    

