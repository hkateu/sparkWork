package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType


case class Series(id: String, area: String, measure: String, title: String)
case class LAData(id: String, year: Int, period: String, value: Double)

object NOAAData extends App{
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val series = spark.read.textFile("data/laSeries.csv").map{ line =>
    val p = line.split("\t").map(_.trim)
    Series(p(0), p(2), p(3), p(6))
  }.cache()
  series.show()
  /* val tschema = StructType(Array(
                             StructField("sid", StringType),
                             StructField("date", DateType),
                             StructField("mtype", StringType),
                             StructField("value", DoubleType)
                           )) */
//  val data2017 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("data/2017data.csv")
//  data2017.show()

  // check data types of columns read in
//  data2017.schema.printTreeString()

//  val tmax2017 = data2017.filter($"mtype" === "TMAX").drop("mtype").withColumnRenamed("value", "tmax")
//  val tmin2017 = data2017.filter('mtype === "TMIN").drop("mtype").withColumnRenamed("value", "tmin")
//  val combinedTemps2017 = tmax2017.join(tmin2017, Seq('sid, 'date, ('tmax + 'tmin)/2))

spark.stop()
}
