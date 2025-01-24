#e) PySpark code to determine the language spoken by the maximum number of people in each landmass

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext("local","Pyspark Practice Problem")

    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))
    