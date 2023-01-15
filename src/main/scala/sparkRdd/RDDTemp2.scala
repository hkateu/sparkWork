package sparkRdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class TempData2(
    day: Int,
    doy: Int,
    month: Int,
    year: Int,
    precip: Double,
    snow: Double,
    tave: Double,
    tmax: Double,
    tmin: Double
)

object TempData2 {
  def toDoubleOrNeg(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case _: NumberFormatException => -1
    }
  }
}

object RDDTempData2 extends App {
  val conf = new SparkConf()
    .setAppName("Temp Data")
    .setMaster("local[*]")
    .set("spark.executor.heartbeatInterval", "200000")
    .set("spark.network.timeout", "300000")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val lines = sc.textFile("MN212142_9392.csv").filter(!_.contains("Day"))

  val data = lines
    .flatMap { line =>
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty
      else
        Seq(
          TempData2(
            p(0).toInt,
            p(1).toInt,
            p(2).toInt,
            p(4).toInt,
            TempData2.toDoubleOrNeg(p(5)),
            TempData2.toDoubleOrNeg(p(6)),
            p(7).toDouble,
            p(8).toDouble,
            p(9).toDouble
          )
        )
    }
    .cache()
  println(data.max()(Ordering.by(_.tmax)))
  println(data.reduce((td1, td2) => if (td1.tmax > td2.tmax) td1 else td2))

  val rainyCount = data.filter(_.precip >= 1).count()
  println(
    s"There are $rainyCount days. This is ${rainyCount * 100.0 / data.count()} percent"
  )

  val (rainySum, rainyCount2) = data.aggregate(0.0 -> 0)(
    { case ((sum, cnt), td) =>
      if (td.precip < 1.0) (sum, cnt) else (sum + td.tmax, cnt + 1)
    },
    { case ((s1, c1), (s2, c2)) =>
      (s1 + s2, c1 + c2)
    }
  )
  println(s"The average rainy temperature is ${rainySum / rainyCount2}")

  val rainyTemps =
    data.flatMap(td => if (td.precip < 1.0) Seq.empty else Seq(td.tmax))
  println(
    s"The average rainy temperature is ${rainyTemps.sum / rainyTemps.count}"
  )

  val monthGroups = data.groupBy(_.month)

  val monthlyTemp = monthGroups.map { case (m, days) =>
    m -> days.foldLeft(0.0)((sum, td) => sum + td.tmax) / days.size
  }
  // monthlyTemp.collect.sortBy(_._1) foreach println

  // using double Rdds
  println(s"Stdev of highs: ${data.map(_.tmax).stdev()}")
  println(s"Stdev of lows: ${data.map(_.tmin).stdev()}")
  println(s"Stdev of averages: ${data.map(_.tave).stdev()}")

  val keyedByYear = data.map(td => td.year -> td)
  val averageTempsByYear = keyedByYear.aggregateByKey(0.0 -> 0)(
    { case ((sum, cnt), td) => (sum + td.tmax, cnt + 1) },
    { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
  )

  val averageTempsByYearArray = averageTempsByYear.collect()
  val averageTempsByYearData = averageTempsByYearArray.map({ case (k, v) =>
    k -> v._1 / v._2
  })
  println(averageTempsByYearData.maxBy(_._2))

  sc.stop()
}
