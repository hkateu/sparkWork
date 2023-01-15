package SparkTheDefinitiveGuide

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{window, column, desc, col}

object streamingExample extends App {
  println(util.Properties.versionString)

  val spark =
    SparkSession.builder().appName("test").master("local").getOrCreate()
  import spark.implicits._

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  val staticDataFrame = spark.read
    .format("csv")
    // .option("recursiveFileLookup", "true")
    .option("header", "true")
    .option("inferSchema", "true")
    // .load("data/retail-data/by-day-few")
    .load("data/retail-data/by-day-few/*.csv")

  staticDataFrame.createOrReplaceTempView("retail_data")
  val staticSchema = staticDataFrame.schema

  println(s"we got ${staticDataFrame.count()} number of rows")

  staticDataFrame.printSchema()

  staticDataFrame
    .selectExpr(
      "CustomerID",
      "Quantity * UnitPrice as total_cost",
      "InvoiceDate"
    )
    .groupBy('CustomerID, window('InvoiceDate, "1 day"))
    .sum("total_cost")
    .show(5)

  val streamingDataFrame = spark.readStream
    .schema(staticSchema)
    // .option("recursiveFileLookup", "true")
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")
    // .csv("data/retail-data/by-day-few")
    .load("data/retail-data/by-day-few/*.csv")

  println(s"is it streaming: ${streamingDataFrame.isStreaming} ")

  val purchaseByCustomerPerHour = streamingDataFrame
    .selectExpr(
      "CustomerID",
      "Quantity * UnitPrice as total_cost",
      "InvoiceDate"
    )
    .groupBy('CustomerID, window('InvoiceDate, "1 day"))
    .sum("total_cost")

//  val testDf = streamingDataFrame.groupBy('CustomerID).sum("Quantity")

  purchaseByCustomerPerHour.writeStream
    .format("memory")
    .queryName("customer_purchases")
    .outputMode("complete")
    .start()

//testDf.writeStream.format("memory").queryName("testedDf").outputMode("complete").start()

  spark.stop()
}
