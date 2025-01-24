# - [ ] c) total number of people earning over 50k for each countries

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext("local","Pyspark Practice Problem")

    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))

    lines = lines.map(lambda x:(x[13],(x[14],1)))

    lines = lines.filter(lambda x: x[1][0] == ' >50K')

    lines = lines.reduceByKey(lambda x,y: (x[0],x[1]+y[1]))

    lines = lines.map(lambda x: (x[0],x[1][1]))

    lines.saveAsTextFile("/output")