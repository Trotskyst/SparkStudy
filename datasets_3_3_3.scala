import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object datasets_3_3_3 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("datasets_3_3_3")
    .master("local")
    .getOrCreate()

  //  DataFrame

  val nameCorrect = (columnName: String) => regexp_replace(
    coalesce(col(columnName), lit("")),
    "\n|\r", ""
  )

  val reviewsSplit = lit(
    regexp_replace(
      split(
        coalesce(col("CompanyReviews"), lit("0")),
        " "
      )(0),
      "\\W+", "")
  ).cast("int")

  val jobIndustryDF = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "true",
      "multiline" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/AiJobsIndustry.csv"
    ))
    .load()
    .na
    .drop(Seq("CompanyReviews"))
    .withColumn("Job", nameCorrect("JobTitle"))
    .withColumn("Company", nameCorrect("Company"))
    .withColumn("ReviewCount", reviewsSplit)

  // Ищем максимум отзывов
  val getDFMaxCount = (df: DataFrame, fieldName: String)=>
    df
    .where(col(fieldName) =!= "")
    .groupBy(fieldName)
    .agg(sum(col("ReviewCount")).as("count"))
    .orderBy(col("count").desc)
    .first()
    .get(0)
    .toString

  // Находим локации
  val getLocationDF = (df: DataFrame, filterCondition: Column) =>
    df
    .filter(filterCondition)
    .groupBy("Location")
    .agg(sum(col("ReviewCount")).as("count"))

  // Находим локации с минимумом или максимумом отзывов
  val getLocationCount = (df: DataFrame, orderExpression: Column) =>
    df
    .orderBy(orderExpression, col("Location"))
    .first()

  // Формируем строку с результатом
  val getDFResultLine = (
    df: DataFrame,
    fieldType: String,
    name: String,
    locationRow: Row,
    countType: String
  ) =>
    df
    .filter(
      col(fieldType) === name &&
      col("Location") === locationRow.get(0)
    )
    .select(
      lit(name) as "name",
      lit(fieldType) as "type",
      lit(locationRow.get(0)) as "location",
      lit(locationRow.get(1)) as "count",
      lit(countType) as "count_type"
    )
    .limit(1)

  // Ищем максимум отзывов
  val companyDFMaxCount = getDFMaxCount(
    jobIndustryDF,
    "Company"
  )
  val jobDFMaxCount = getDFMaxCount(
    jobIndustryDF,
    "Job"
  )

  // Находим локации по компании и специальности с максимумом отзывов
  val companyLocationDF = getLocationDF(
    jobIndustryDF,
    col("Company") === companyDFMaxCount
  )
  val jobLocationDF = getLocationDF(
    jobIndustryDF,
    col("Job") === jobDFMaxCount
  )

  // Находим локации с минимумом и максимумом отзывов
  // по компании с максимумом отзывов
  val companyLocationMaxCountDF = getLocationCount(
    companyLocationDF,
    col("count").desc
  )
  val companyLocationMinCountDF = getLocationCount(
    companyLocationDF,
    col("count")
  )
  // по специальности с максимумом отзывов
  val jobLocationMaxCountDF = getLocationCount(
    jobLocationDF,
    col("count").desc
  )
  val jobLocationMinCountDF = getLocationCount(
    jobLocationDF,
    col("count")
  )

  // Формируем строку с результатом
  val companyMaxDF = getDFResultLine(
    jobIndustryDF,
    "company",
    companyDFMaxCount,
    companyLocationMaxCountDF,
    "max"
  )
  val companyMinDF = getDFResultLine(
    jobIndustryDF,
    "company",
    companyDFMaxCount,
    companyLocationMinCountDF,
    "min"
  )
  val jobMaxDF = getDFResultLine(
    jobIndustryDF,
    "job",
    jobDFMaxCount,
    jobLocationMaxCountDF,
    "max"
  )
  val jobMinDF = getDFResultLine(
    jobIndustryDF,
    "job",
    jobDFMaxCount,
    jobLocationMinCountDF,
    "min"
  )

  companyMaxDF
    .unionAll(companyMinDF)
    .unionAll(jobMaxDF)
    .unionAll(jobMinDF)
    .show()

  // DataSet

  case class JobLoad (
    JobTitle: String,
    Company: String,
    Location: String,
    CompanyReviews: String,
    Link: String
  )

  case class Job (
   Job: String,
   Company: String,
   Location: String,
   ReviewCount: Int
  )

  case class JobGroup (
    Name: String,
    Count: Int
  )

  import spark.implicits._

  val jobIndustryDS = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "true",
      "multiline" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/AiJobsIndustry.csv"
    ))
    .load()
    .na
    .drop(List("Location", "CompanyReviews"))
    .as[JobLoad]

  val nameDSCorrect = (name: String) => name.replaceAll("\n|\r", "")

  val reviewsDSSplit = (review: String) => review
    .split(" ")(0)
    .replaceAll("\\W+", "")
    .toInt

  val getJob = (job: JobLoad) =>
    Job (
      nameDSCorrect(job.JobTitle),
      nameDSCorrect(job.Company),
      job.Location,
      reviewsDSSplit(job.CompanyReviews)
    )

  // Готовим данные для дальнейшей обработки
  val jobDS: Dataset[Job] = jobIndustryDS
    .filter(
      col("JobTitle").isNotNull &&
      col("Company").isNotNull
    )
    .map(getJob)

  // Находим компании и спеуиальности с максимумом отзывов
  val getNameMaxCount = (
    ds: Dataset[Job],
    fieldType: String
  ) =>
    ds
    .groupByKey(fieldType match {
      case "company" => _.Company
      case "job" => _.Job
    })
    .mapGroups((key, value) => JobGroup(key, value.map(_.ReviewCount).sum))
    .orderBy(col("Count").desc)
    .first()
    .Name

  // Находим локации
  val getLocations = (
   ds: Dataset[Job],
   filterCondition: Column
 ) =>
    ds
    .filter(filterCondition)
    .groupByKey(_.Location)
    .mapGroups(
      (key, value) => JobGroup(key, value.map(_.ReviewCount).sum)
    )

  // Находим локации с минимальным или максимальным количеством отзывов
  val getLocationExtreme = (
   ds: Dataset[JobGroup],
   sortCountExpression: Column
 ) =>
    ds
    .orderBy(sortCountExpression, col("Name"))
    .first()

  // Формируем строку с результатом
  def getDSResultLine = (
    ds: Dataset[Job],
    fieldType: String,
    name: String,
    location: JobGroup,
    countType: String
  ) =>
    ds
      .filter(
        col(fieldType) === name &&
        col("Location") === location.Name
      )
      .select(
        lit(name) as "name",
        lit(fieldType) as "type",
        lit(location.Name) as "location",
        lit(location.Count) as "count",
        lit(countType) as "count_type"
      )
      .limit(1)

  // Находим максимум отзывов по компаниям и специальностям
  val companyDSMaxCount = getNameMaxCount(
    jobDS,
    "company"
  )
  val jobDSMaxCount = getNameMaxCount(
    jobDS,
    "job"
  )

  // Находим локации по компании и специальности
  val companyLocationDS = getLocations(
    jobDS,
    col("Company") === companyDSMaxCount
  )
  val jobLocationDS = getLocations(
    jobDS,
    col("Job") === jobDSMaxCount
  )

  // Находим локации с минимумом и максимумом отзывов
  // по компании
  val companyLocationMaxCountDS = getLocationExtreme(
    companyLocationDS,
    col("Count").desc
  )
  val companyLocationMinCountDS = getLocationExtreme(
    companyLocationDS,
    col("Count")
  )
  // по специальности
  val jobLocationMaxCountDS = getLocationExtreme(
    jobLocationDS,
    col("Count").desc
  )
  val jobLocationMinCountDS = getLocationExtreme(
    jobLocationDS,
    col("Count")
  )

  val companyMaxDS = getDSResultLine(
    jobDS,
    "company",
    companyDSMaxCount,
    companyLocationMaxCountDS,
    "max"
  )
  val companyMinDS = getDSResultLine(
    jobDS,
    "company",
    companyDSMaxCount,
    companyLocationMinCountDS,
    "min"
  )
  val jobMaxDS = getDSResultLine(
    jobDS,
    "job",
    jobDSMaxCount,
    jobLocationMaxCountDS,
    "max"
  )
  val jobMinDS = getDSResultLine(
    jobDS,
    "job",
    jobDSMaxCount,
    jobLocationMinCountDS,
    "min"
  )

  companyMaxDS
    .unionAll(companyMinDS)
    .unionAll(jobMaxDS)
    .unionAll(jobMinDS)
    .show()

  spark.stop()
}
