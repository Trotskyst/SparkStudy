import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

// Параметры загрузки
object LoadParameter {
  val fileNameTransactions: String = "src/main/resources/transactions.csv"
  val fileNameCountries: String = "src/main/resources/country_codes.csv"
}

// Параметры выгрузки
object SaveParameter {
  val fileNameResult: String = "src/main/resources/result.parquet"
}

// Условия задания
object TaskCondition {
  val countryName = "United Kingdom"
  val date = "2018-12-01"
}

object optim_6_5_3_RDD extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("optim_6_5_2_RDD")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  case class Country (
    name: String,
    code: String
  )

  case class Transaction (
    transactionNo: String,
    date: String,
    productName: String,
    countryCode: String
  )

  case class Result (
    countryCode: String,
    countryName: String,
    date: String,
    mostPopularProduct: String,
    avgTransaction: BigDecimal
  )

  // Загрузка страны
  val loadCsvCountry = (sparkContext: SparkContext, fileName: String, taskConditionCountryName: String) =>
    sparkContext.textFile(fileName)
      .map { line =>
        val fields = line.split(",")
        Country(fields(0), fields(1))
      }
      .filter(country => country.name == taskConditionCountryName)
      .first()

  // Условие загрузки транзакций
  val loadCsvTransactionCondition = (
    value: Transaction,
    conditionDate: String,
    conditionCountry: String
  ) =>
    value.date != null &&
    value.countryCode != null &&
    value.date == conditionDate &&
    value.countryCode == conditionCountry

  // Загрузка транзакций
  val loadCsvTransaction = (
    sparkContext: SparkContext,
    fileName: String,
    conditionDate: String,
    conditionCountry: String
  ) =>
    sparkContext.textFile(fileName)
      .map { line =>
        val fields = line.split(",")
        Transaction(fields(0), fields(1), fields(3), fields(7))
      }
      .filter(value => loadCsvTransactionCondition(value, conditionDate, conditionCountry))

  // Сохранение результата
  def saveResult (
    fileName: String,
    countryCode: String,
    countryName: String,
    date: String,
    mostPopularProduct: String,
    avgTransaction: BigDecimal
  ): Unit = {
    val result = Seq(
      Result(
        countryCode,
        countryName,
        date,
        mostPopularProduct,
        avgTransaction
      )
    )
    result
      .toDF(
        "countryCode",
        "countryName",
        "date",
        "mostPopularProduct",
        "avgTransaction")
      .write
      .mode(SaveMode.Overwrite)
      .save(fileName)
  }

  // Получить вектор из элементов колонки RDD
  def getColumn(rdd: RDD[Transaction], columnName: String): Vector[String] = {
    rdd.map(line => columnName match {
        case "products" => line.productName
        case "transactions" => line.transactionNo
      })
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

  val sc = spark.sparkContext

  // Страна
  val country = loadCsvCountry(sc, LoadParameter.fileNameCountries, TaskCondition.countryName)

  // Транзакции
  val transactionsRDD = loadCsvTransaction(
    sc,
    LoadParameter.fileNameTransactions,
    TaskCondition.date,
    sc.broadcast(country).value.code)

  // Вектор с продуктами
  val products = getColumn(transactionsRDD, "products")
  // Вектор с транзакциями
  val transactions = getColumn(transactionsRDD, "transactions")

  // Наиболее часто покупаемый товар
  val mostPopularProduct = getMostPopularProduct(products)

  // Сколько товаров в среднем посетители приобретают за одну транзакцию
  val avgTransaction =  getAvgTransaction(
    transactions,
    1)
  
  println(s"Наиболее часто покупаемый товар - $mostPopularProduct")
  println(s"Сколько товаров в среднем посетители приобретают за одну транзакцию - $avgTransaction")

  // Сохранение результата в файл
  saveResult(
    SaveParameter.fileNameResult,
    country.code,
    TaskCondition.countryName,
    TaskCondition.date,
    mostPopularProduct,
    avgTransaction)

//  System.in.read()

  spark.stop()
}
