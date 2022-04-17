import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object toDF_2_3_6 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("toDF_2_3_6")
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

  val df = bikeSharingDF
    .withColumn("is_workday",
      when(
        col("HOLIDAY") === "Holiday" &&
        col("FUNCTIONING_DAY") === "No", 0)
      .otherwise(1))
    .select("HOLIDAY", "FUNCTIONING_DAY", "is_workday")
    .distinct()
  df.show()

  spark.stop()
}
