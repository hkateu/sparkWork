package SparkTheDefinitiveGuide

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{date_format, to_date}
import org.apache.spark.sql.types.{StructType, StructField, StringType, TimestampType, DoubleType, IntegerType}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans

object KmeansExample extends App{
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.sql.shuffle.partitions", "5")
  val tschema = StructType(Array(
                             StructField("InvoiceNo", IntegerType),
                             StructField("StockCode", StringType),
                             StructField("Description", StringType),
                             StructField("Quantity", DoubleType),
                             StructField("InvoiceDate", TimestampType),
                             StructField("UnitPrice", DoubleType),
                             StructField("CustomerID", DoubleType),
                             StructField("Country", StringType)
                           )
  )
//  val data2017 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("data/2017data.csv")
  val staticDataFrame = spark.read.format("csv")
    .option("header","true")
    .schema(tschema).option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .load("data/retail-data/by-day-few/*.csv")

  staticDataFrame.printSchema()

  val preppedDataFrame = staticDataFrame.na.fill(0).withColumn("day_of_week", date_format('InvoiceDate, "EEEE"))
  .coalesce(5)

  preppedDataFrame.printSchema()

  val trainedDataFrame = preppedDataFrame.filter('InvoiceNo > "536401")
  val testDataFrame = preppedDataFrame.filter('InvoiceNo <= "536401")

  println(s"trained: ${trainedDataFrame.count()}")
  println(s"test: ${testDataFrame.count()}")

  trainedDataFrame.show(5)

  val indexer = new StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index")
  //val indexed = indexer.fit(trainedDataFrame).transform(trainedDataFrame)
  //indexed.show(5)

  val encoder = new OneHotEncoder().setInputCol("day_of_week_index").setOutputCol("day_of_week_encoded")
  val vectorAssembler = new VectorAssembler().setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded")).setOutputCol("features")
  val transformationPipeline = new Pipeline().setStages(Array(indexer, encoder, vectorAssembler))
  val fittedPipeline = transformationPipeline.fit(trainedDataFrame)
  val transformedTraining = fittedPipeline.transform(trainedDataFrame)

  //transformedTraining.show(5)
  transformedTraining.cache()
  val kmeans = new KMeans().setK(20).setSeed(1L)
  val kmModel = kmeans.fit(transformedTraining)

  val transformTest = fittedPipeline.transform(testDataFrame)
  println("print our test results")
  transformTest.show(5)

  spark.stop()
}
