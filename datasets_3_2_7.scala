import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit}

object datasets_3_2_7 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("datasets_3_2_7")
    .master("local")
    .getOrCreate()

  val shoesDF = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/athletic_shoes.csv"
    ))
    .load()

  case class Shoes(
                    item_category: String,
                    item_name: String,
                    item_after_discount: String,
                    item_price: String,
                    percentage_solds: Int,
                    item_rating: Int,
                    item_shipping: String,
                    buyer_gender: String
                  )

  import spark.implicits._

  val isNotNullItem =
    !col("item_name").isNull &&
    !col("item_category").isNull

  val shoesDS = shoesDF
    .select(
      col("item_category"),
      col("item_name"),
      coalesce(col("item_after_discount"), col("item_price")) as "item_after_discount",
      col("item_price"),
      col("percentage_solds"),
      coalesce(col("item_rating"), lit(0)) as "item_rating",
      col("item_shipping"),
      coalesce(col("buyer_gender"), lit("unknown")) as "buyer_gender",
    )
    .filter(isNotNullItem)
    .na
    .fill("n/a", List(
      "item_category",
      "item_name",
      "item_price",
      "percentage_solds",
      "item_shipping"
    ))
    .as[Shoes]

  spark.stop()
}
