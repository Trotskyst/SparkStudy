import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import io.circe.parser
import io.circe.generic.auto._

object rdd_4_3_6 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Rdds")
    .config("spark.master", "local[1]")
    .getOrCreate()

  // Чтение файла
  val readFileRDD = (sparkContext: SparkContext, fileName: String) => sparkContext.textFile(fileName)

  val fileName = "src/main/resources/amazon_products.json"

  val sc = spark.sparkContext

  val fileRDD = readFileRDD(sc, fileName)

  case class JsonProduct (
    uniq_id: Option[String],
    product_name: Option[String],
    manufacturer: Option[String],
    price: Option[String],
    number_available: Option[String],
    number_of_reviews: Option[Int]
  )

  val parsedProduct = fileRDD
    .map(x => {
      val parsedJson = parser.decode[JsonProduct](x)
      parsedJson match {
        case Left(_) => JsonProduct(null, null, null, null, null, null)
        case Right(json) => json
      }
    })

  parsedProduct
    .filter(_.uniq_id != null)
    .foreach(x => println(
      s"uniq_id = ${x.uniq_id.getOrElse("")}\n" +
      s"product_name = ${x.product_name.getOrElse("")}\n" +
      s"manufacturer = ${x.manufacturer.getOrElse("")}\n" +
      s"price = ${x.price.getOrElse("")}\n" +
      s"number_available = ${x.number_available.getOrElse("")}\n" +
      s"number_of_reviews = ${x.number_of_reviews.getOrElse(0)}\n"))

  spark.stop()
}