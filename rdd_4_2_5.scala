import org.apache.commons.math3.util.Precision.round
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object rdd_4_2_5 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Rdds")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  case class Avocado (
    id: Int,
    date: String,
    avgPrice: Double,
    volume: Double,
    year: String,
    region: String
  )

  case class Months (
    name: String
  )

  case class RegionVolumeCount(
    region: String,
    volumeCount: (Double, Int)
  )

  case class RegionVolume(
    region: String,
    avgVolume: Double
  )

  val avocadosRDD = sc.textFile("src/main/resources/avocado.csv")
    .map(line => line.split(","))
    .filter(values => values(0) != "id")
    .map(values => Avocado (
      values(0).toInt,
      values(1),
      values(2).toDouble,
      values(3).toDouble,
      values(12),
      values(13)
    ))

  println(s"1. Количество уникальных регионов = ${
    avocadosRDD
      .map(_.region)
      .distinct()
      .count()
  }")

  println("2. Продажи авокадо, сделанные после 2018-02-11:")
  avocadosRDD
    .filter(_.date > "2018-02-11")
    .foreach(println)

  println(s"3. Месяц, который чаще всего представлен в статистике: ${
    avocadosRDD
    .filter(_.date != "")
    .map(_.date.split("-"))
    .filter(_.length == 3)
    .map(values => Months(values(1)))
    .countByValue
    .head
    ._1
    .name
  }")

  implicit val avocadoOrdering: Ordering[Avocado] =
    Ordering.fromLessThan[Avocado]((s1: Avocado, s2: Avocado) => s1.avgPrice < s2.avgPrice)

  println(s"4. Максимальное значение avgPrice = ${avocadosRDD.max().avgPrice}")
  println(s"Минимальное  значение avgPrice = ${avocadosRDD.min().avgPrice}")

  println(s"5. Средний объем продаж для каждого региона:")
  avocadosRDD
    .groupBy(_.region)
    .map(x => (
      x._1,
      x._2.map(y => (y.volume, 1))
    ))
    .map(x => RegionVolumeCount(
      x._1,
      x._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    ))
    .map(x => RegionVolume(
      x.region,
      round(x.volumeCount._1 / x.volumeCount._2, 2)
    ))
    .foreach(x => println(s"${x.region} - ${x.avgVolume}"))

  spark.stop()
}
