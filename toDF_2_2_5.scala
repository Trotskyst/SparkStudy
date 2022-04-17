import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

object toDF_2_2_5 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("toDF_2_2_5")
    .master("local")
    .getOrCreate()

  val moviesSchema = StructType(Seq(
    StructField("_c0", IntegerType),
    StructField("show_id", StringType),
    StructField("type", StringType),
    StructField("title", StringType),
    StructField("director", StringType),
    StructField("cast", StringType),
    StructField("country", StringType),
    StructField("date_added", DateType),
    StructField("release_year", IntegerType),
    StructField("rating", StringType),
    StructField("duration", IntegerType),
    StructField("listed_in", StringType),
    StructField("description", StringType),
    StructField("year_added", IntegerType),
    StructField("month_added", StringType),
    StructField("season_count", IntegerType)
  ))

  val moviesDF = spark.read
    .format("csv")
    .schema(moviesSchema)
    .options(Map(
      "header" -> "true",
      "path" -> "src/main/resources/movies.csv"
    ))
    .load()

  moviesDF.printSchema()

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data")

  spark.stop()
}
