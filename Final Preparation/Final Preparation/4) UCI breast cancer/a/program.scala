// - [ ] a) Scala code to find clusters based on the diagnosis, tumor radius and age group using the k-means, consider a cluster number = 10


import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

val text = sc.textFile("/input/input.txt")

val records = text.map(line => line.split(","))
val tuples = records.map(x=>(x(2),x(3),x(4),x(5)))
val parsedData = tuples.map(line =>Vectors.dense(line._1.toDouble,line._2.toDouble,line._3.toDouble,line._4.toDouble))

parsedData.collect.foreach(println)
val kmeans = new KMeans()
kmeans.setK(2)
val model = kmeans.run(parsedData)

model.clusterCenters.foreach(println)
model.predict(parsedData).foreach(println)

val testData = model.predict(Vectors.dense(17.99,10.38,122.8,1001))
println(testData)

// 1. Class: no-recurrence-events, recurrence-events
//    2. age: 10-19, 20-29, 30-39, 40-49, 50-59, 60-69, 70-79, 80-89, 90-99.
//    3. menopause: lt40, ge40, premeno.
//    4. tumor-size: 0-4, 5-9, 10-14, 15-19, 20-24, 25-29, 30-34, 35-39, 40-44,
//                   45-49, 50-54, 55-59.
//    5. inv-nodes: 0-2, 3-5, 6-8, 9-11, 12-14, 15-17, 18-20, 21-23, 24-26,
//                  27-29, 30-32, 33-35, 36-39.
//    6. node-caps: yes, no.
//    7. deg-malig: 1, 2, 3.
//    8. breast: left, right.
//    9. breast-quad: left-up, left-low, right-up,	right-low, central.
//   10. irradiat:	yes, no.

