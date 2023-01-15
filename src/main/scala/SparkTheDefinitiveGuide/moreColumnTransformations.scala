package SparkTheDefinitiveGuide
import org.apache.spark.sql.functions.{col}

object moreColumnTransformations extends App {
  println("Chapter 5")
  val spark = sparkUtil.getSpark("BasicSpark")
  import spark.implicits._

  val flights =
    spark.read.format("json").load("data/flight/json/2015-summary.json")
  flights.show(5)
  flights.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(5)
  flights.printSchema()
  val flights2 = flights.withColumn("count1", 'count.cast("int"))
  flights2.filter('count > 5).filter('count < 10).show(5)
  flights2.filter('ORIGIN_COUNTRY_NAME =!= "Croatia").show(5)
  val uniquiCounts = flights
    .select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
    .distinct()
    .count()
  println(uniquiCounts)
  spark.stop()
}
