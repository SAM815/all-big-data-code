# - [ ] b) PySpark code to find the education level of the maximum number of people earning over 50k

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext("local","Pyspark Practice Problem")

    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))

    lines = lines.map(lambda x:(x[3],(x[14],1)))

    lines = lines.filter(lambda x: x[1][0] == ' >50K')

    lines = lines.reduceByKey(lambda x,y: (x[0],x[1]+y[1]))

    lines = lines.reduceByKey(lambda x,y: x if x[1] > y[1] else y)

    lines = lines.map(lambda x: (x[0],x[1][1]))

    #find max value
    max_value = lines.map(lambda x: x[1]).max()

    #filter out the max value
    lines = lines.filter(lambda x: x[1] == max_value)



    lines.saveAsTextFile("/output")
