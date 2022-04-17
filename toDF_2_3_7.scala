import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object toDF_2_3_7 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("toDF_2_3_7")
    .master("local")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/bike_sharing.csv"
    ))
    .load()

  bikeSharingDF
    .groupBy("Date")
    .agg(
      min("TEMPERATURE").as("min_temp")
      ,max("TEMPERATURE").as("max_temp"))
    .show()

  spark.stop()
}
