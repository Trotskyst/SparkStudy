import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object optim_6_5_2_RDD extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("optim_6_5_2_RDD")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Загрузка файла
  val loadCsv = (sparkContext: SparkContext, fileName: String) =>
    sparkContext.textFile(fileName)
    .map(line => line.split(","))

  // Находим код страны
  def getCountryCode(
    rdd: RDD[Array[String]],
    taskConditionCountryName: String)
  : String = {
    // Находим информацию об искомой стране
    val currentCountry = rdd
      .filter(x => x(0) == taskConditionCountryName)
      .first

    currentCountry(1)
  }

  // Получить вектор из элементов колонки RDD
  def getColumn(rdd: RDD[Array[String]], columnNumber: Int): Vector[String] = {
    val columnRDD = rdd.map(x => Column(x(columnNumber)))
    val columnDF = spark.createDataFrame(columnRDD)

    columnDF
      .map(x => x.getAs[String](0))
      .collect
      .toVector
  }

  // Найти наиболее часто покупаемый товар
  def getMostPopularProduct (productNames: Vector[String]): String = {
    productNames
      .groupBy(identity)
      .mapValues(_.size)
      .toSeq
      .sortWith(_._2 > _._2)
      .head
      ._1
  }

  // Сколько товаров в среднем посетители приобретают за одну транзакцию
  def getAvgTransaction(
    transactions: Vector[String],
    scale: Int)
  : BigDecimal = {
    // Количество операций
    val productCount = transactions.size
    // Количество транзакций
    val transactionCount = transactions
      .distinct
      .size

    BigDecimal(productCount / transactionCount)
      .setScale(scale, BigDecimal.RoundingMode.HALF_UP)
  }

  // Параметры загрузки
  object LoadParameter {
    val FileNameTransactions: String = "src/main/resources/transactions.csv"
    val FileNameCountries: String = "src/main/resources/country_codes.csv"
  }

  // Условия задания
  object TaskCondition {
    val CountryName = "United Kingdom"
    val Date = "2018-12-01"
  }

  case class Column (
    Value: String
  )

  val sc = spark.sparkContext

  // Страны
  val countriesRDD = loadCsv(sc, LoadParameter.FileNameCountries)

  // Код страны
  val currentCountryCode = getCountryCode(
    countriesRDD,
    TaskCondition.CountryName)

  // Транзакции
  val transactionsRDD = loadCsv(sc, LoadParameter.FileNameTransactions)
    .filter(x => x(1) == TaskCondition.Date && x(7) == currentCountryCode)

  // Вектор с продуктами
  val products = getColumn(transactionsRDD, 3)
  // Вектор с транзакциями
  val transactions = getColumn(transactionsRDD, 0)

  // Наиболее часто покупаемый товар
  val mostPopularProduct = getMostPopularProduct(products)

  // Сколько товаров в среднем посетители приобретают за одну транзакцию
  val avgTransaction =  getAvgTransaction(
    transactions,
    1)
  println(s"Наиболее часто покупаемый товар - $mostPopularProduct")
  println(s"Сколько товаров в среднем посетители приобретают за одну транзакцию - $avgTransaction")

//  System.in.read()

  spark.stop()
}
