//Bernal Enciso Jaime Alejandro
//14212006
//Datos Masivos

//1
import org.apache.spark.sql.SparkSession

//2
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

//3
val spar = SparkSession.builder().getOrCreate()

//4
import org.apache.spark.ml.clustering.KMeans

//5
val df = spark.read.option("header", "true").option("inferSchema","true")csv("Wholesale customers data.csv")

//6
val df = dataset.drop("Channel", "Region")

//7
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexer

//8
val assembler = new Vector() .setInputCols(Array("Fresh", "Grocery", "Frozen", "Detergents_paper", "Delicassen")
      .setOutputCol("features")

//9
val features = assembler.transform(df)

//10
val kmeans = new KMeans().setK(3)
val model = kmeans.fit(features)

//11
val WSSE = model.computeCost(df)
println(s"Within set sum of Squared Errors = $WSSE")

//12
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
