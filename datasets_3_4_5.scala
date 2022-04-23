import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

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

  val cars = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/cars.csv")
    .withColumnRenamed("type", "types")
    .as[Car]

  val avgMileageDF = cars
    .agg(
      avg(
        coalesce(col("mileage"), lit(0))
      )
        .as("avg_mileage")
    )

  val monthCountInYear = 12

  val correctedCars = cars.as("a")
    .crossJoin(avgMileageDF)
    .withColumn("years_since_purchase", lit(
      floor(months_between(
        lit(java.time.LocalDate.now),
        coalesce(
          to_date(col("date_of_purchase"), "yyyy-MM-dd"),
          to_date(col("date_of_purchase"), "yyyy MM dd"),
          to_date(col("date_of_purchase"), "yyyy-MMM-dd"),
          to_date(col("date_of_purchase"), "yyyy MMM dd")
        )
      ) / monthCountInYear)
    )
    ).as[CorrectedCar]

  correctedCars.show()
}
