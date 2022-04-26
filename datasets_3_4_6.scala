import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object datasets_3_4_6 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("datasets_3_4_6")
    .master("local")
    .getOrCreate()

  case class Position (
    PositionID: Int,
    Position: String
  )

  import spark.implicits._

  val positionDS = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/hrdataset.csv")
    .select(
      "PositionID",
      "Position"
    ).as[Position]

  val positionsQueries = List(
    "BI",
    "it"
  )

  val positionsQueriesLower = positionsQueries.map(_.toLowerCase)

  def getFilteredPositions(position: String, queries: List[String]): Boolean = {
    queries.exists(query =>
      position.startsWith(query)
    )
  }

  val apiDS: Dataset[Position] = positionDS
    .filter(position => getFilteredPositions(
      position.Position.toLowerCase(),
      positionsQueriesLower
    ))
    .distinct()
    .orderBy("positionID")

  apiDS.show()

  spark.stop()
}
