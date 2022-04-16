import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object toDF_2_1_4 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("toDF_2_1_4")
    .master("local")
    .getOrCreate()

  val videos = Vector(
    ("s9FH4rDMvds", "2020-08-11T22:21:49Z", "UCGfBwrCoi9ZJjKiUK8MmJNw", "2020-08-12T00:00:00Z"),
    ("kZxn-0uoqV8", "2020-08-11T14:00:21Z", "UCGFNp4Pialo9wjT9Bo8wECA", "2020-08-12T00:00:00Z"),
    ("QHpU9xLX3nU", "2020-08-10T16:32:12Z", "UCAuvouPCYSOufWtv8qbe6wA", "2020-08-12T00:00:00Z")
  )

  import spark.implicits._

  val videosDF = videos.toDF("videoId", "publishedAt", "channelId", "trendingDate")
  videosDF.show()
  spark.stop()
}
