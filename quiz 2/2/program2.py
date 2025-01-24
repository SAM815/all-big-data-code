import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    sc = SparkContext("local","Pyspark word count example")

    lines = sc.textFile('/input/input.txt').map(lambda line: line.split(","))

    #mapper and reducer code
    result = lines.map(lambda x: (str(x[1]),(int(x[6]),1))
                       if (int(x[6]) > 1 )
                       else (0,(0,0))
                       )

    result = result.mapValues(lambda x: x[1])
    result = result.reduceByKey(lambda x,y: (x+y))
    result = result.map(lambda x:(1,(x[0],x[1]))).reduceByKey(lambda x,y: x if x[1] > y[1] else y)
    
    result.saveAsTextFile('/output/cancer')


