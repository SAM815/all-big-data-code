import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._


val text = sc.textFile("/input/input.txt")
val records = text.map(x => x.split(","))
val tuples = records.map(x => (x(1),x(5),x(3),x(4),x(6)))
val parsedata = tuples.map(s => Vectors.dense(s._1.toDouble, s._2.toDouble, s._3.toDouble, s._4.toDouble,s._5.toDouble))

val kmeans = new KMeans().setSeed(1)
kmeans.setK(10)

val model = kmeans.run(parsedata)

val testdata = model.predict(Vectors.dense(3,6,29,3,6))
println(testdata)

val testdata2 = model.predict(Vectors.dense(3,4,84,8,0))
println(testdata2)