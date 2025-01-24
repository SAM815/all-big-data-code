import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel

val text = sc.textFile("/input/input.txt")
val splitdata = text.map(line => line.split(","))


val parseddata = splitdata.map(x=> (if (x(2) == "?") {-1} else {x(2).toDouble},
                                (if(x(1)== "M") {1} else {0},
                                )))

val data = parseddata.map{x => 
                            val featureVector = Vectors.dense(x._1)
                            val label = x._2
                            println(featureVector)
                            println(label)
                            LabeledPoint(label,featureVector)

}

val categoricalFeaturesInfo = Map[Int,Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32
val numClasses = 2
val model = DecisionTree.trainClassifier(data,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)

val testData = Vectors.dense(66.0)
val prediction = model.predict(testData)
println("Learned classification tree model: " + model.toDebugString)
println("Prediction for test data " + testData + " : " + (if(prediction == 1.0) "M" else "B"))








