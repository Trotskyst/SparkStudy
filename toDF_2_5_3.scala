import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.annotation.tailrec

object toDF_2_5_3 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Количество выводимых в итоге строк
  val topCount = 20

  val spark = SparkSession.builder()
    .appName("toDF_2_5_3")
    .master("local")
    .getOrCreate()

  //Загрузка файлов
  @tailrec
  def loadCsv(
    seasons: Vector[String],
    result: Vector[sql.DataFrame] = Vector()
  ): Vector[sql.DataFrame] =
  {
    if (seasons.isEmpty) result
    else {
      val seasonDF = spark.read
        .option("inferSchema", "true")
        .csv(s"src/main/resources/subtitles_s${seasons.head}.json")
      loadCsv(seasons.tail, result :+ seasonDF)
    }
  }

  import spark.implicits._

  // Загружаем сезоны
  val seasonsDF = loadCsv(Vector("1", "2"))

  // Разбиваем на слова
  val splitDS = seasonsDF
    .map(
      _.flatMap(
        _.getString(0)
        .toLowerCase()
        .split("\\W+")
      )
    )

  // Подсчитываем слова
  val groupDS = splitDS.map(_
    .filter(col("value") =!= "")
    .groupBy("value")
    .agg(count("value").as("count"))
    .orderBy(col("count").desc)
  )

  // Получаем топ слов
  def getTopWords(
    season1: List[Row],
    season2: List[Row],
    topLimit: Int = 20,
    id: Int = 0,
    words: Vector[(String, Int, Int, String, Int)] = Vector()
  ): Vector[(String, Int, Int, String, Int)] = {
    if (
      id > topLimit ||
      season1.isEmpty ||
      season2.isEmpty
    ) words
    else {
      val row = (
        season1.head.get(0).toString,
        season1.head.get(1).toString.toInt,
        id,
        season2.head.get(0).toString,
        season2.head.get(1).toString.toInt,
      )
      getTopWords(
        season1.tail,
        season2.tail,
        topLimit,
        id + 1,
        words :+ row
      )
    }
  }

  // Получаем топ слов
  val topWords = getTopWords(
    groupDS(0).collect().toList,
    groupDS(1).collect().toList,
    topCount
  )

  // Получаем DataFrame
  val topWordsDF = spark
    .createDataFrame(topWords)
    .toDF(
      "w_s1",
      "cnt_s1",
      "id",
      "w_s2",
      "cnt_s2"
    )

  // Записываем результат в файл
  topWordsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("resources/data/wordcount")

  spark.stop()
}