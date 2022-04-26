import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

import java.time.LocalDate

object datasets_3_4_5 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("datasets_3_4_5")
    .master("local")
    .getOrCreate()

  case class Car(
    id: Int,
    price: Int,
    brand: String,
    types: String,
    mileage: Option[Double],
    color: String,
    date_of_purchase: String,
  )

  case class CorrectedCar(
   id: Int,
   price: Int,
   brand: String,
   types: String,
   mileage: Option[Double],
   color: String,
   date_of_purchase: String,
   avg_mileage: Double,
   years_since_purchase: Long
 )

  import spark.implicits._

  val carsDS = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/cars.csv")
    .withColumnRenamed("type", "types")
    .as[Car]

  def avgMileage(cars: Dataset[Car]): Double = {
    val sumMileage: (Double, Int) = cars
      .map(car => (car.mileage.getOrElse(0.0), 1))
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    sumMileage._1 / sumMileage._2
  }

  val mileage = avgMileage(carsDS)

  def convertDateOfPurchase(date: String, today: LocalDate): LocalDate = {
    import java.time.format.DateTimeFormatter
    import scala.util.Try

    val dateFormats = List(
      "dd/mm/yyyy",
      "yyyy-MM-dd",
      "yyyy MM dd",
      "yyyy-MMM-dd",
      "yyyy MMM dd"
    )

    dateFormats.dropWhile(
      format =>
        Try(
          LocalDate.parse(date, DateTimeFormatter.ofPattern(format))
        ).isFailure
    )
      .headOption match {
      case Some(fmt) => LocalDate.parse(date, DateTimeFormatter.ofPattern(fmt))
      case None => today
    }

  }

  def yearsSincePurchase(date: String): Long = {
    import java.time.Period

    val today = java.time.LocalDate.now

    Period.between(
      convertDateOfPurchase(date, today),
      today
    ).getYears
  }

  def getCorrectedCar(car: Car, mileage: Double): CorrectedCar = {
    CorrectedCar(
      car.id,
      car.price,
      car.brand,
      car.types,
      car.mileage,
      car.color,
      car.date_of_purchase,
      mileage,
      yearsSincePurchase(car.date_of_purchase)
    )
  }

  val correctedCarsDS: Dataset[CorrectedCar] = carsDS.map(car => getCorrectedCar(car, mileage))

  correctedCarsDS.show()

  spark.stop()
}
