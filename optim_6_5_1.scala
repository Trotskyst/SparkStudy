import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{broadcast, lit}

object optim_6_5_1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("optim_6_5_1")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  object Params {
    val fileNameCodes: String = "src/main/resources/cancel_codes.csv"
    val fileNameHotels: String = "src/main/resources/hotel_bookings.csv"
  }

  // Загрузка файла
  def loadCSV(fileName: String): DataFrame =
    spark.read
      .format("csv")
      .options(Map(
        "inferSchema" -> "true",
        "header" -> "true",
        "path" -> fileName
      ))
      .load()

  val codesDF = loadCSV(Params.fileNameCodes)
  val hotelsDF = loadCSV(Params.fileNameHotels)

  // Условие соединения датафреймов
  val joinCondition = hotelsDF.col("is_canceled") === codesDF.col("id")
  // В датафрейме с отелями заявка отменена
  val isHotelCanceled = hotelsDF.col("reservation_status") === lit("Canceled")
  // Код является кодом отмены
  val isCancelCode = codesDF.col("is_cancelled") === lit("yes")

  val errorsDS = hotelsDF.join(broadcast(codesDF), joinCondition)
    .filter(isHotelCanceled =!= isCancelCode)

  val errorsCount = errorsDS.count()
  println(errorsCount) //ОТВЕТ - 42996

  spark.stop()
}
