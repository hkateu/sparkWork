package SparkTheDefinitiveGuide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HelloSpark extends App {
  println(util.Properties.versionString)

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on spark version ${spark.version}")
  //spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  /*
  val myRange = spark.range(1000).toDF("number")
  val divideBy5 = myRange.where("number % 5 = 0")
  divideBy5.show(10)

  val myRange2 = spark.range(100).toDF("nums")
  val divideBy10 = myRange2.filter('nums % 10 === 0)
  divideBy10.show()
   */

  val flightData2015 = spark.read.option("inferSchema", "true").option("header", "true").csv("data/flight/2015-summary.csv")
  println(s"flight data rows were ${flightData2015.count()}")
  flightData2015.show(5)
  flightData2015.schema.printTreeString()
  flightData2015.printSchema()

  spark.conf.set("spark.sql.shuffle.partitions", "5")
  flightData2015.sort('count.desc).show(5)
  flightData2015.groupBy('DEST_COUNTRY_NAME).agg(sum('count).alias("countssss")).sort('countssss.desc).show(10)
  flightData2015.sort('count).explain()

  flightData2015.createOrReplaceTempView("flight_data_2015")

  val sqlway = spark.sql("""SELECT DEST_COUNTRY_NAME as dest, sum(count) as flight FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME ORDER BY flight DESC""")

  sqlway.show(10)

  flightData2015.select(max("count").as("maxCount")).show(1)

  flightData2015.groupBy('DEST_COUNTRY_NAME).sum("count").withColumnRenamed("sum(count)", "destCount").sort('destCount.desc).limit(4).show()

  //val csvTestFile = flightData2015.groupBy('DEST_COUNTRY_NAME).sum("count").withColumnRenamed("sum(count)", "destCount").sort('destCount.desc).limit(4)
  //csvTestFile.write.format("csv").mode("overwrite").option("sep","\t").save("flight-data-grouped.tsv")


  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)
  val flightDF = spark.read.parquet("/data/flight/flight.parquet")
  val nflights = flightDF.as[Flight]
  nflights.show(5)

  spark.stop()

}
