package SparkTheDefinitiveGuide

import sparkUtil.getSpark
//import org.apache.spark.sql.functions.col

object BasicStructuredOperations extends App {
  val spark = getSpark("BasicSpark")
  import spark.implicits._
  // val flightPath = "data/flight/json/2015-summary.json"
  val flightpath2 = "data/flight/2014-summary.csv"
  val flights2014 =
    spark.read.format("csv").option("header", "true").load(flightpath2)
  val flightFiltered = flights2014
    .filter('DEST_COUNTRY_NAME === "United States")
    .filter('count <= 10)
  flightFiltered.show(10)

  val withReplacement = false
  val fraction = 0.3
  val seed = 3
  val sampledFlights = flightFiltered.sample(withReplacement, fraction, seed)
  sampledFlights.orderBy('DEST_COUNTRY_NAME.desc).show(10)

  println("show results")
  sampledFlights.show(10)
  println("Head results")
  sampledFlights.head(10).foreach(println)
  println("take results")
  sampledFlights.take(10).foreach(println)
  println("take as list")
  println(sampledFlights.takeAsList(10))
  println("limit result")
  sampledFlights.limit(10).show()
  println("first result")
  println(sampledFlights.first())
  // val df = spark.read.format("json").load(flightPath)
  // df.show(5)
  // df.coalesce(1).write.option("header", "true").option("mode","overwrite").option("sep",",").csv("data/test-summary2015.csv")
  spark.stop()
}
