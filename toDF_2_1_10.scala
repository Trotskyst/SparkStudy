import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

object toDF_2_1_10 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("toDF_2_1_10")
    .master("local")
    .getOrCreate()

  val restaurantExSchema = StructType(Seq(
    StructField("average_cost_for_two", LongType),
    StructField("cuisines", StringType),
    StructField("deeplink", StringType),
    StructField("has_online_delivery", IntegerType),
    StructField("is_delivering_now", IntegerType),
    StructField("menu_url", StringType),
    StructField("name", StringType),
    StructField("url", StringType),
    StructField("user_rating",
      StructType(Seq(
        StructField("aggregate_rating", StringType),
        StructField("rating_color", StringType),
        StructField("rating_text", StringType),
        StructField("votes", StringType)
      )))
  ))

  val restaurantExDF = spark.read
    .format("json")
    .schema(restaurantExSchema)
    .load("src/main/resources/restaurant_ex.json")

  restaurantExDF.show(1)
  restaurantExDF.printSchema()
  spark.stop()
}
