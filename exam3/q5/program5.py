# Q5. Download the Heart Disease data set from
# https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.hungarian.data
# Data Description:
# https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/heart-disease.names
# a. Implement a machine learning method on scala/spark to predict if a patient has a heart disease.
# Columns with no input ("?") should be replaced with 0.
# b. Test your model for the following feature vector -
# [65,1,4,130,275,0,1,115,1,1,2,?,?]

import sys

from pyspark.sql.types import DoubleType
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import Vectors

def convert(input):
    if input == '?':
        input =  0.0
    else:
        input = float(input)
    return input

def parsePoint(line):
    values = [convert(x) for x in line.split(',')]
    return LabeledPoint(values[13], values[0:13])

sc = SparkContext("local", "Random Forest")
data = sc.textFile("/input/input.txt")
parsedData = data.map(parsePoint)

model = RandomForest.trainClassifier(parsedData, numClasses=2, categoricalFeaturesInfo={},
                                        numTrees=3, featureSubsetStrategy="auto",
                                        impurity='gini', maxDepth=4, maxBins=32)

testData = ([convert(x) for x in[65,1,4,130,275,0,1,115,1,1,2,'?','?']])

prediction = model.predict(testData)
f = open("out5.txt", "w+")
f.write("Prediction: "+str(prediction))
f.close()
