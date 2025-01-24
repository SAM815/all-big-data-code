import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":

    sc = SparkContext("local", "Pyspark dataframe")
    # convert sparkContext to sqlContext
    sqlContext = SQLContext(sc)
    # reading to dataFrame
    df = sqlContext.read.format("com.databricks.spark.csv").options(header = "false", inferschema = "true").load("/input/input.txt")
    df.show()

    vecAssembler = VectorAssembler(inputCols=["_c1","_c5","_c3","_c4","_c6"], outputCol="features")
    new_df = vecAssembler.transform(df)

    kmeans = KMeans(k=10, seed=1)
    model = kmeans.fit(new_df.select('features'))
    prediction = model.transform(new_df)
    prediction.show()

    evaluator = ClusteringEvaluator()
    score =  evaluator.evaluate(prediction)
    print( "Score: " + str(score))

    # testing the model
    prediction_test1 = model.predict(Vectors.dense([3,6,29,3,6]))
    print("Prediction one: " + str(prediction_test1))
    prediction_test2 = model.predict(Vectors.dense([3,4,84,8,0]))
    print("Prediction two: " + str(prediction_test2))

