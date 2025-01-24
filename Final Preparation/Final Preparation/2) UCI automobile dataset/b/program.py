import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    #create spark context 
    sc = SparkContext("local","Pyspark exam practice example")
    
    #read data from a text file and split each words into two
    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))

    lines = lines.map(lambda x:(x[5],(x[2],int(x[25]) if x[25] != '?' else 0)))
    # lines = lines.filter(lambda x: x[1][2] == 'two')
    # lines = lines.mapValues(lambda x: (x[0],x[2]))
    
    # result = lines.reduceByKey(lambda x,y: x if x[1] > y[1] else y)
    # result = result.map(lambda x: (x[1][0],x[1][1]))

    result = lines.reduceByKey(lambda x,y: x if x[1] > y[1] else y)
    result = result.filter(lambda x: x[0] == 'two')


    #save the counts to output
    result.saveAsTextFile("/output/b")
