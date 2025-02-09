import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val spark = SparkSession.builder().appName("LogisticRegressionExample").getOrCreate()

val df = spark.read.format("csv")
            .option("header", "false")
            .load("input/input.txt")
            .select("_c1","_c2","_c3","_c4","_c5" )
            .toDF("diag", "radius", "texture", "perimeter", "area")

val featureCols = Array("radius", "texture", "perimeter", "area")
val assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features")

val assembledData = assembler.transform(df)
val indexer = new StringIndexer().setInputCol("diag").setOutputCol("label")

val Data = indexer.fit(assembledData).transform(assembledData)

val Array(trainingData, testData) = Data.randomSplit(Array(0.7, 0.3))

val lr = new LogisticRegression()

val lrModel = lr.fit(trainingData)

val predictions = lrModel.transform(testData)

val evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
val testError = (1.0-accuracy) * 100

printIn("TestError =  + $testError%")





