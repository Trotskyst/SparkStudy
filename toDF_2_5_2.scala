import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object toDF_2_5_2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("toDF_2_5_2")
    .master("local")
    .getOrCreate()

  val mallCustomersDF = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/mall_customers.csv"
    ))
    .load()

  val incomeDF = mallCustomersDF
    .withColumn("Age", col("Age") + 2)
    .where(col("Age") between (30, 35))
    .groupBy("Gender", "Age")
    .agg(bround(avg("Annual Income (k$)"), 1) as "avg_Annual_Income")
    .withColumn("gender_code", when(col("Gender") === "Male", 1).otherwise(0))
    .orderBy("Gender", "Age")

  incomeDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/customers")

  spark.stop()
}