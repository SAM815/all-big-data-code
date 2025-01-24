# - [ ] d) PySpark code to find the minimum price of four doors sedan


import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext("local","Pyspark Practice Problem")

    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))
    
    # door -5
    # price -25

    lines = lines.map(lambda x:((x[2], x[5]),x[25])).mapValues(lambda x: (10000000) if x == "?" else (float(x)))

    # lines = lines.map(lambda x: (x if x[1][0] == 'four' else (x[0],(x[1][0],x[1][1],10000000))))

    lines = lines.filter(lambda x: x[0][1] == 'four')

    lines = lines.map(lambda x: (1,(x[0],x[1])))

    lines = lines.reduceByKey(lambda x,y: x if x[1] < y[1]  else y)

    lines.saveAsTextFile("/output/min")

