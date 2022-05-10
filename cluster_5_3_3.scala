package com.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

case class Pizza (
  order_type: String,
  address_id: Int
)

case class OrderTotal (
  order_type: String,
  orders_total: Long,
  address_id: Int,
  orders_cnt: Long
)

object PizzaApp {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Неверно указан путь")
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("PizzaApp")
      .master("local")
      .getOrCreate()

    object LoadColumns {
      val orderType = "order_type"
      val addressId = "address_id"
    }

    object Params {
      val loadFileName: String = args(0)
      val saveDirectoryName: String = args(1)
    }

    import spark.implicits._

    val pizzaDS = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(Params.loadFileName)
      .select(
        LoadColumns.orderType,
        LoadColumns.addressId
      )
      .as[Pizza]

    val resultDS = pizzaDS
      .groupByKey(_.order_type)
      .mapGroups {
        (orderType: String, iter: Iterator[Pizza]) =>
          val addresses = iter.toVector
          val addressCount = addresses
            .groupBy(_.address_id)
            .maxBy(_._2.size)

          OrderTotal(
            orderType,
            addresses.size,
            addressCount._1,
            addressCount._2.size
          )
      }

    resultDS.show()

    resultDS
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("path", Params.saveDirectoryName)
      .save
  }
}
