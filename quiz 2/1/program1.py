import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    sc = SparkContext("local","Pyspark word count example")

    words = sc.textFile('/input/input.txt').flatMap(lambda line:line.split(","))

    wordcounts = words.map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)

    #highest occurance
    highest = wordcounts.map(lambda x: (1, (x[0], x[1]))).reduceByKey(lambda x,y: x if x[1]>y[1] else y)

    highest.saveAsTextFile('/output/wordcount')
    

    