import org.apache.commons.math3.util.Precision.round
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date
import scala.annotation.tailrec

object rdd_4_3_5 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Rdds")
    .config("spark.master", "local[1]")
    .getOrCreate()

  case class Log (
    id: Long,
    host: String,
    time: Long,
    method: String,
    url: String,
    response: String,
    bytes: Long
  )

  case class SortedGroupedData(
    name: String,
    count: Int
  )

  val replaceQuoteChar = "!@#"
  val separator = ','

  // Разделить строку на слова с учётом вложенности в кавычки
  val splitLine = (line: String, lineSeparator: Char, replaceQuoteChar: String) => {
    @tailrec
    def loop(
        chars: Vector[Char],
        separator: Char = ',',
        inQuote: Boolean = false,
        newChars: Vector[String] = Vector())
      : Vector[String] = {
        if (chars.isEmpty) newChars
        else {
          val quoted = if (chars.head == '\"') !inQuote else inQuote
          val currentChar =
            if (chars.head == separator && quoted)
              replaceQuoteChar
            else
              chars.head.toString
          loop(chars.tail, separator, quoted, newChars :+ currentChar)
        }
    }
    loop(line.toVector, lineSeparator).mkString.split(lineSeparator)
  }

  // Преобразование даты, представленной в виде числа, в указанном формате
  val formatDate = (timestamp: Long, dateFormat: SimpleDateFormat) =>
    dateFormat.format(new Date(timestamp * 1000))

  // Чтение файла
  val readFileRDD = (sparkContext: SparkContext, fileName: String) => sparkContext.textFile(fileName)

  // Сортировка данных после группировки по количеству
  def getSortedGroupedData[T](rdd: RDD[(String, Iterable[T])]): RDD[SortedGroupedData] = {
    rdd.map(x => (
      x._2.size,
      x._1
    ))
    .sortByKey(false)
    .map(x => SortedGroupedData(
      x._2,
      x._1
    ))
  }

  val locale = java.util.Locale.forLanguageTag("ru")
  val formatter = new java.text.SimpleDateFormat("EEEE", locale)
  val fileName = "src/main/resources/logs_data.csv"

  val sc = spark.sparkContext

  val fileRDD = readFileRDD(sc, fileName)
    .map(splitLine(_, separator, replaceQuoteChar))

  val logRDD = fileRDD
      .filter(values => values(0) != "")
      .map(values => Log(
        values(0).toLong,
        values(1),
        values(2).toLong,
        values(3),
        values(4),
        values(5),
        values(6).toLong
      ))

  val lineCountAll = fileRDD.count()
  val lineCountLoaded = logRDD.count()
  val lineCountSkipped = lineCountAll - lineCountLoaded
  println(s"1. Всего строк в файле: $lineCountAll")
  println(s"Загружено строк: $lineCountLoaded")
  println(s"Пропущено строк: ${lineCountSkipped}")

  println("2. Количество записей для кодов ответа (response):")
  val responseCountRDD = getSortedGroupedData(logRDD
    .groupBy(_.response))
  responseCountRDD.foreach(x => println(s"${x.name} - ${x.count}"))

  println("3. Статистика по размеру ответа (bytes):")
  val bytesRDD = logRDD.map(x => x.bytes)
  val bytesSum = bytesRDD.sum.toLong
  val bytesAvg = round(bytesSum / bytesRDD.count, 2)
  val bytesMax = bytesRDD.max
  val bytesMin = bytesRDD.min
  println(s"Сумма: $bytesSum")
  println(s"Среднее: $bytesAvg")
  println(s"Максимум: $bytesMax")
  println(s"Минимум: $bytesMin")

  val hostsRDD = logRDD.groupBy(_.host)

  println("4. Количество уникальных хостов:")
  println(hostsRDD.count)

  println("5. Наиболее часто встречающеся хосты (host):")
  val topCount = 3
  val hostTop = getSortedGroupedData(hostsRDD)
    .take(topCount)
  hostTop
    .map(x => SortedGroupedData(
      x.name.replace(replaceQuoteChar, separator.toString),
      x.count
    ))
    .foreach(x => println(s"${x.name} - ${x.count}"))

  println("6. Топ дней недели:")
  val filterResponse = "404"
  val topWeekdayCount = 3
  val dateResponse = getSortedGroupedData(
      logRDD
      .filter(_.response == filterResponse)
      .groupBy(x => formatDate(x.time, formatter))
    )
    .take(topWeekdayCount)
    .map(x => SortedGroupedData(
      x.name.capitalize,
      x.count
    ))
  dateResponse.foreach(x => println(s"${x.name} - ${x.count}"))

  spark.stop()
}
