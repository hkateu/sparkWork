package sparkRdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Area(code: String, text: String)
case class Series(id: String, area: String, measure: String, title: String)
case class LAData(id: String, year: Int, period: Int, value: Double)
object RDDUnemployment extends App {
  val conf = new SparkConf()
    .setAppName("Temp Data")
    .setMaster("local[*]")
    .set("spark.executor.heartbeatInterval", "200000")
    .set("spark.network.timeout", "300000")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  val areas = sc
    .textFile("data/laArea.csv")
    .filter(!_.contains("area_type"))
    .map { line =>
      val p = line.split("\t").map(_.trim)
      Area(p(1), p(2))
    }
    .cache()

  areas.take(5) foreach println

  val series = sc
    .textFile("data/laSeries.csv")
    .filter(!_.contains("area_code"))
    .map { line =>
      val p = line.split("\t").map(_.trim)
      Series(p(0), p(2), p(3), p(6))
    }
    .cache()

  series.take(5) foreach println

  val data = sc
    .textFile("data/minesota.csv")
    .filter(!_.contains("year"))
    .map { line =>
      val p = line.split("\t").map(_.trim)
      LAData(p(0), p(1).toInt, p(2).drop(1).toInt, p(3).toDouble)
    }
    .cache()

  data.take(5) foreach println

  val rates = data.filter(x => x.id.endsWith("03"))
  val decadeGroups = rates.map(d => (d.id, d.year / 10) -> d.value)
  val decadeAverages = decadeGroups
    .aggregateByKey(0.0 -> 0)(
      { case ((s, c), d) =>
        (s + d, c + 1)
      },
      { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
    )
    .mapValues(t => t._1 / t._2)
  decadeAverages.take(5) foreach println

  val maxDecade = decadeAverages
    .map { case ((id: String, dec: Int), av: Double) => id -> (dec, av) }
    .reduceByKey((x, y) => if (x._1 >= y._1) x else y)

  val seriesPairs = series.map(s => s.id -> s.title)
  val joinedMaxDecades = seriesPairs.join(maxDecade)

  joinedMaxDecades.take(10) foreach println

  val dataByArea =
    joinedMaxDecades.mapValues { case (a, (b, c)) => (a, b, c) }.map {
      case (k, v) => k.drop(3).dropRight(2) -> v
    }

  val fullyJoined = areas.map(a => a.code -> a.text).join(dataByArea)
  fullyJoined.take(10) foreach println

  sc.stop()
}
