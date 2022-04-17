import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object toDF_2_3_2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("toDF_2_3_2")
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

  val smallBikeSharingDF = bikeSharingDF.select("Hour", "TEMPERATURE", "HUMIDITY", "WIND_SPEED")
  smallBikeSharingDF.show(3)

  spark.stop()
}
