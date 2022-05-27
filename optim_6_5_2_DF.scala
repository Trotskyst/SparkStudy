import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col

object optim_6_5_2_DF extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("optim_6_5_2_DF")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Загрузка файла
  val loadCsv = (fileName: String) =>
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(fileName)

  // Находим код страны
  def getCountryCode(
    df: DataFrame,
    fieldCountry: String,
    fieldCountryCode: String,
    taskConditionCountryName: String
  ): Int = {
    // Условие фильтрации списка стран
    val countryFilterCondition = col(fieldCountry) === taskConditionCountryName

    df.filter(countryFilterCondition)
      .head
      .getAs[Int](fieldCountryCode)
  }

  // Получить вектор из элементов колонки DataFrame
  def getColumn(df: DataFrame, columnNumber: Int): Vector[String] =
    df
      .map(x => x.getAs[String](columnNumber))
      .collect
      .toVector

  // Найти наиболее часто покупаемый товар
  def getMostPopularProduct (productNames: Vector[String]): String =
    productNames
      .groupBy(identity)
      .mapValues(_.size)
      .toSeq
      .sortWith(_._2 > _._2)
      .head
      ._1

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

  // Поля
  object Field {
    val CountryCode = "CountryCode"
    val Country = "Country"
    val Date = "Date"
    val TransactionNo = "TransactionNo"
    val ProductName = "ProductName"
    val Count = "count"
  }

  // Страны
  val countriesDF = loadCsv(LoadParameter.FileNameCountries)

  // Код страны
  val currentCountryCode = getCountryCode(
    countriesDF,
    Field.Country,
    Field.CountryCode,
    TaskCondition.CountryName)

  // Условие фильтрации загруженных транзакций
  val transactionLoadCondition =
    col(Field.Date) === TaskCondition.Date and
    col(Field.CountryCode) === currentCountryCode

  // Транзакции
  val transactionsDF = loadCsv(LoadParameter.FileNameTransactions)
    .filter(transactionLoadCondition)

  // Вектор с продуктами
  val products = getColumn(transactionsDF, 3)
  // Вектор с транзакциями
  val transactions = getColumn(transactionsDF, 0)

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