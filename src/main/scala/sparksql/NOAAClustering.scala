package sparksql

import org.apache.spark.sql.SparkSession

case class Station(
    /*sid: String, lat: Double, lon:Double, elev: Double,*/ name: String
)
object NOAAClustering extends App {
  val spark =
    SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
  val stations = spark.read
    .textFile("data/noaaStations.csv")
    .map { line =>
      // val id = line.substring(0, 11)
      // val lat = line.substring(12, 20).trim.toDouble
      // val lon = line.substring(21, 30).trim.toDouble
      // val elev = line.substring(31, 37).trim.toDouble
      val name = line.substring(41, 71).trim()
      Station( /*id, lat, lon, elev,*/ name)
    }
    .cache()

  stations.show()

  spark.stop()
}
