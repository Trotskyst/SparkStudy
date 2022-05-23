import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, col, concat, lit, rand}
import org.apache.spark.sql.types.IntegerType

object optim_6_4_2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("optim_6_4_2")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  object Params {
    val fileNameFreeLancers: String = "src/main/resources/freelancers.csv"
    val fileNameOffers: String = "src/main/resources/offers.csv"
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

  // Сформировать ключ с солью
  val getSaltKey = () =>
    concat(
      col("category"),
      lit("_"),
      col("city"),
      lit("_"),
      col("salt"))

  // Количество повторений
  val salt = 32

  // DataFrame с числами от 0 до salt-1
  val saltDF = spark.range(salt).toDF("salt")

  // 1. Загружаем фрилансеров
  // 2. Перемножаем их с saltDF
  // 3. Вычисляем ключ с солью
  // 4. Удаляем лишние колонки
  val freeLancersDF = loadCSV(Params.fileNameFreeLancers)
    .crossJoin(saltDF)
    .withColumn("saltKey", getSaltKey())
    .drop("category", "city", "salt")

  // 1. Загружаем заказы
  // 2. Добавляем колонку с солью - случайное число от 0 до salt-1
  // 3. Вычисляем ключ с солью
  // 4. Удаляем лишние колонки
  val offersDF = loadCSV(Params.fileNameOffers)
    .withColumn("salt", (rand * salt).cast(IntegerType))
    .withColumn("saltKey", getSaltKey())
    .drop("category", "city", "salt")

  // В примере с неоптимальным решением меняем ключ Seq("category", "city") на новый ключ - "saltKey"
  freeLancersDF.join(offersDF, "saltKey")
    .filter(functions.abs(freeLancersDF.col("experienceLevel") - offersDF.col("experienceLevel")) <= 1)
    .groupBy("id")
    .agg(avg("price").as("avgPrice"))
    .show()

  System.in.read()

  spark.stop()
}
