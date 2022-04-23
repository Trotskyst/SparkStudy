import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object datasets_3_4_6 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("datasets_3_4_6")
    .master("local")
    .getOrCreate()

  val hr = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/hrdataset.csv")

  import spark.implicits._

  val positionsQueries = List(
    "BI",
    "it"
  )

  val api = hr.as("a")
    .join(positionsQueries.toDF().as("b"),
      lower(col("a.Position"))
        .startsWith(
          lower(col("b.value"))
        ),
      "inner")
    .select(
      col("a.PositionID") as "positionID",
      col("a.Position") as "position"
    )
    .distinct()
    .orderBy("positionID")

  api.show()

  spark.stop()
}
