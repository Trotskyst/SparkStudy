import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object optim_6_1_11 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("optim_6_1_9")
    .master("local")
    .getOrCreate()

  val employeesDF = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/employee.csv"
    ))
    .load()

  employeesDF
    .groupBy("department")
    .avg("salary")
    .explain()

  spark.stop()
  
/*
id,city,country,salary,department
0,London,England,123,IT
 */
}
