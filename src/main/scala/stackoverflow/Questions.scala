import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Questions extends App {
  def ageCheck(age: Int): String = if (age > 18) "Y" else "N"
  val spark =
    SparkSession.builder().appName("testing").master("local[*]").getOrCreate()
  spark.stop()
}
