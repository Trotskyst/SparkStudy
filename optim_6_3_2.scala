import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, col, count, lit, regexp_replace, sum}

object optim_6_3_2 extends App {
  //<editor-fold desc="Подготовка">
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("optim_6_1_9")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  def addColumn(df: DataFrame, n: Int): DataFrame = {
    val columns = (1 to n).map(col => s"col_$col")
    columns.foldLeft(df)((df, column) => df.withColumn(column, lit("n/a")))
  }

  val data1 = (1 to 500000).map(i => (i, i * 100))
  val data2 = (1 to 10000).map(i => (i, i * 1000))

  import spark.implicits._

  val df1 = data1.toDF("id","salary").repartition(5)
  val df2 = data2.toDF("id","salary").repartition(10)

  //</editor-fold>

  //<editor-fold desc="Способ 1 - добавим колонки и объединим партицированные датафреймы">

  val dfWithColumns = addColumn(df2, 10)
  val joinedDF1 = dfWithColumns.join(df1, "id")

//  joinedDF1.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- Project [id#18, salary#19, n/a AS col_1#22, n/a AS col_2#26, n/a AS col_3#31, n/a AS col_4#37, n/a AS col_5#44, n/a AS col_6#52, n/a AS col_7#61, n/a AS col_8#71, n/a AS col_9#82, n/a AS col_10#94, salary#8]
//  +- SortMergeJoin [id#18], [id#7], Inner
//  :- Sort [id#18 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(id#18, 200), ENSURE_REQUIREMENTS, [id=#25]
//  :     +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [id=#18]
//  :        +- LocalTableScan [id#18, salary#19]
//  +- Sort [id#7 ASC NULLS FIRST], false, 0
//  +- Exchange hashpartitioning(id#7, 200), ENSURE_REQUIREMENTS, [id=#26]
//  +- Exchange RoundRobinPartitioning(5), REPARTITION_BY_NUM, [id=#20]
//  +- LocalTableScan [id#7, salary#8]

  //</editor-fold>

  //<editor-fold desc="Способ 2 - заново разделим данные, но на этот раз разделять будем по id; затем объединим данные и добавим колонки">

  val repartitionedById1 = df1.repartition(col("id"))
  val repartitionedById2 = df2.repartition(col("id"))

  val joinedDF2 = repartitionedById2.join(repartitionedById1, "id")
  val dfWithColumns2 = addColumn(joinedDF2, 10)

//  dfWithColumns2.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- Project [id#18, salary#19, salary#8, n/a AS col_1#123, n/a AS col_2#128, n/a AS col_3#134, n/a AS col_4#141, n/a AS col_5#149, n/a AS col_6#158, n/a AS col_7#168, n/a AS col_8#179, n/a AS col_9#191, n/a AS col_10#204]
//  +- SortMergeJoin [id#18], [id#7], Inner
//  :- Sort [id#18 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(id#18, 200), REPARTITION_BY_COL, [id=#18]
//  :     +- LocalTableScan [id#18, salary#19]
//  +- Sort [id#7 ASC NULLS FIRST], false, 0
//  +- Exchange hashpartitioning(id#7, 200), REPARTITION_BY_COL, [id=#20]
//  +- LocalTableScan [id#7, salary#8]

  //</editor-fold>

//  Начинаем исследование

  //<editor-fold desc="Эксперимент 1">
//  1. добавляем колонку до join
//  2. увеличиваем salary в 10 раз
//  3. заполняем col_1 и делаем замену символов
//  4. сортируем
//  Итог -не поменялось почти ничего, только операция увеличения в 10 раз добавила строчку и distinct привел к HashAggregate

  val experiment01DS = addColumn(repartitionedById1, 10)
    .withColumn("salary", lit(10) * col("salary"))
    .withColumn("col_1", lit("abc"))
    .withColumn("col_1", regexp_replace(col("col_1"), "a", "b"))
    .distinct()
    .sort("salary")
  val experiment01_joinedDF2 = repartitionedById2.join(experiment01DS, "id")

//  experiment01_joinedDF2.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- Project [id#18, salary#19, salary#303, bbc AS col_1#329, n/a AS col_2#222, n/a AS col_3#227, n/a AS col_4#233, n/a AS col_5#240, n/a AS col_6#248, n/a AS col_7#257, n/a AS col_8#267, n/a AS col_9#278, n/a AS col_10#290]
//  +- SortMergeJoin [id#18], [id#7], Inner
//  :- Sort [id#18 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(id#18, 200), REPARTITION_BY_COL, [id=#26]
//  :     +- LocalTableScan [id#18, salary#19]
//  +- Sort [id#7 ASC NULLS FIRST], false, 0
//  +- HashAggregate(keys=[n/a#355, n/a#356, n/a#357, n/a#358, n/a#359, salary#303, id#7, n/a#360, n/a#361, bbc#362, n/a#363, n/a#364], functions=[])
//  +- HashAggregate(keys=[n/a AS n/a#355, n/a AS n/a#356, n/a AS n/a#357, n/a AS n/a#358, n/a AS n/a#359, salary#303, id#7, n/a AS n/a#360, n/a AS n/a#361, bbc AS bbc#362, n/a AS n/a#363, n/a AS n/a#364], functions=[])
//  +- Project [id#7, (10 * salary#8) AS salary#303]
//  +- Exchange hashpartitioning(id#7, 200), REPARTITION_BY_COL, [id=#28]
//  +- LocalTableScan [id#7, salary#8]

  //</editor-fold>

  //<editor-fold desc="Эксперимент 2">
  //  1. Делаем distinct сразу после добавления колонок
  //  Итог - план не усложнился, добавился HashAggregate

  val experiment02_joinedDF = repartitionedById2.as("r2").join(repartitionedById1.as("r1"), "id")
  val experiment02_joined2DS = addColumn(experiment02_joinedDF, 10)
    .distinct()

//  experiment02_joined2DS.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- HashAggregate(keys=[n/a#453, n/a#454, n/a#455, n/a#456, n/a#457, salary#19, salary#8, id#18, n/a#458, n/a#459, n/a#460, n/a#461, n/a#462], functions=[])
//  +- HashAggregate(keys=[n/a AS n/a#453, n/a AS n/a#454, n/a AS n/a#455, n/a AS n/a#456, n/a AS n/a#457, salary#19, salary#8, id#18, n/a AS n/a#458, n/a AS n/a#459, n/a AS n/a#460, n/a AS n/a#461, n/a AS n/a#462], functions=[])
//  +- Project [id#18, salary#19, salary#8]
//  +- SortMergeJoin [id#18], [id#7], Inner
//  :- Sort [id#18 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(id#18, 200), REPARTITION_BY_COL, [id=#23]
//  :     +- LocalTableScan [id#18, salary#19]
//  +- Sort [id#7 ASC NULLS FIRST], false, 0
//  +- Exchange hashpartitioning(id#7, 200), REPARTITION_BY_COL, [id=#25]
//  +- LocalTableScan [id#7, salary#8]

  //</editor-fold>

  //<editor-fold desc="Эксперимент 3">
  //  1. После добавления колонок преобразуем в датасет, преобразуем в другой датасет и distinct
  //  Итог - последние 2 операции - дорогие
  //  map(x => (x.id, x.salary)) вызвало десериализацию и сериализацию
  //  distinct - вызвал перемешивание

  case class Experiment03 (id: Int, salary: Int)
  val experiment03_joinedDF = repartitionedById2.as("r2").join(repartitionedById1.as("r1"), "id")
  val experiment03_joinedDS = addColumn(experiment03_joinedDF, 10)
    .select("r2.id", "r2.salary")
    .as[Experiment03]
    .map(x => (x.id, x.salary))
    .distinct()

//  experiment03_joinedDS.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- HashAggregate(keys=[_1#562, _2#563], functions=[])
//  +- Exchange hashpartitioning(_1#562, _2#563, 200), ENSURE_REQUIREMENTS, [id=#53]
//  +- HashAggregate(keys=[_1#562, _2#563], functions=[])
//  +- SerializeFromObject [knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1 AS _1#562, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2 AS _2#563]
//  +- MapElements optim_6_3_2$$$Lambda$1523/0x0000000100ca8840@31b67d61, obj#561: scala.Tuple2
//  +- DeserializeToObject newInstance(class optim_6_3_2$Experiment03), obj#560: optim_6_3_2$Experiment03
//  +- Project [id#18, salary#19]
//  +- SortMergeJoin [id#18], [id#7], Inner
//  :- Sort [id#18 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(id#18, 200), REPARTITION_BY_COL, [id=#32]
//  :     +- LocalTableScan [id#18, salary#19]
//  +- Sort [id#7 ASC NULLS FIRST], false, 0
//  +- Exchange hashpartitioning(id#7, 200), REPARTITION_BY_COL, [id=#34]
//  +- LocalTableScan [id#7]

  //</editor-fold>

  //<editor-fold desc="Эксперимент 4">
//  1. Делаем не join, а union
//  Итог - упростился физический план

  val experiment04_joinedDS = repartitionedById2.union(repartitionedById1)

//  experiment04_joinedDS.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- Union
//    :- Exchange hashpartitioning(id#18, 200), REPARTITION_BY_COL, [id=#15]
//  :  +- LocalTableScan [id#18, salary#19]
//  +- Exchange hashpartitioning(id#7, 200), REPARTITION_BY_COL, [id=#17]
//  +- LocalTableScan [id#7, salary#8]

  //</editor-fold>

  //<editor-fold desc="Эксперимент 5">
//  1. Не делаем repartition по id, а сразу объединяем
//  Итог - повторился план способа 1 - тоже делается repartition сначала по числу, а потом по id

  val experiment05_joinedDF = df1.join(df2, "id")

//  experiment05_joinedDF.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- Project [id#7, salary#8, salary#19]
//  +- SortMergeJoin [id#7], [id#18], Inner
//  :- Sort [id#7 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(id#7, 200), ENSURE_REQUIREMENTS, [id=#45]
//  :     +- Exchange RoundRobinPartitioning(5), REPARTITION_BY_NUM, [id=#38]
//  :        +- LocalTableScan [id#7, salary#8]
//  +- Sort [id#18 ASC NULLS FIRST], false, 0
//  +- Exchange hashpartitioning(id#18, 200), ENSURE_REQUIREMENTS, [id=#46]
//  +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [id=#40]
//  +- LocalTableScan [id#18, salary#19]

  //</editor-fold>

  //<editor-fold desc="Эксперимент 6">
  //  1. Повторяем эксперимент 1, но добавление колонок и всю обработку делаем после join
  //  Итог - если сортировать по конкретному полю,
  //  то сортировка полученного после соединения датафрейма -
  //  дорогой процесс, он потребовал провести rangepartitioning

  val experiment06_joinedDF = repartitionedById2.as("r2").join(repartitionedById1.as("r1"), "id")
  val experiment06_joined2DS = addColumn(experiment06_joinedDF, 10)
    .withColumn("r1.salary", lit(10) * col("r1.salary"))
    .withColumn("col_1", lit("abc"))
    .withColumn("col_1", regexp_replace(col("col_1"), "a", "b"))
    .distinct()
    .sort("r1.id")

//  experiment06_joined2DS.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- Project [id#18, salary#19, salary#8, bbc AS col_1#703, n/a AS col_2#583, n/a AS col_3#589, n/a AS col_4#596, n/a AS col_5#604, n/a AS col_6#613, n/a AS col_7#623, n/a AS col_8#634, n/a AS col_9#646, n/a AS col_10#659, r1.salary#673]
//  +- Sort [id#719 ASC NULLS FIRST], true, 0
//  +- Exchange rangepartitioning(id#719 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [id=#47]
//  +- HashAggregate(keys=[n/a#720, n/a#721, n/a#722, n/a#723, r1.salary#673, n/a#724, salary#19, salary#8, id#18, n/a#725, n/a#726, bbc#727, n/a#728, n/a#729], functions=[first(id#7, false)])
//  +- HashAggregate(keys=[n/a AS n/a#720, n/a AS n/a#721, n/a AS n/a#722, n/a AS n/a#723, r1.salary#673, n/a AS n/a#724, salary#19, salary#8, id#18, n/a AS n/a#725, n/a AS n/a#726, bbc AS bbc#727, n/a AS n/a#728, n/a AS n/a#729], functions=[partial_first(id#7, false)])
//  +- Project [id#18, salary#19, salary#8, (10 * salary#8) AS r1.salary#673, id#7]
//  +- SortMergeJoin [id#18], [id#7], Inner
//  :- Sort [id#18 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(id#18, 200), REPARTITION_BY_COL, [id=#29]
//  :     +- LocalTableScan [id#18, salary#19]
//  +- Sort [id#7 ASC NULLS FIRST], false, 0
//  +- Exchange hashpartitioning(id#7, 200), REPARTITION_BY_COL, [id=#31]
//  +- LocalTableScan [id#7, salary#8]

  //</editor-fold>

  //<editor-fold desc="Эксперимент 7">
  //  1. Применяем к результату после соединения аггрегирующие функции
  //  Итог - аггрегирующие функции приводят к усложнению физического плана из-за операции перемешивания

  val experiment07_joinedDF = repartitionedById2.as("r2").join(repartitionedById1.as("r1"), "id")
  val experiment07_joined2DS = experiment07_joinedDF
    .agg(avg("r2.id").as("id2"))
//    .agg(count("r2.id"))

//  experiment07_joined2DS.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- HashAggregate(keys=[], functions=[avg(id#18)])
//  +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#38]
//  +- HashAggregate(keys=[], functions=[partial_avg(id#18)])
//  +- Project [id#18]
//  +- SortMergeJoin [id#18], [id#7], Inner
//  :- Sort [id#18 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(id#18, 200), REPARTITION_BY_COL, [id=#23]
//  :     +- LocalTableScan [id#18]
//  +- Sort [id#7 ASC NULLS FIRST], false, 0
//  +- Exchange hashpartitioning(id#7, 200), REPARTITION_BY_COL, [id=#25]
//  +- LocalTableScan [id#7]

  //</editor-fold>

  //<editor-fold desc="Эксперимент 8">

  //  1. Делаем аггрегацию над уже перераспределенным датасетом
  //  Итог - снова выполняется перемешивание
  val experiment08DF = repartitionedById1
    .agg(avg("id").as("id2"))

//  experiment08DF.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- HashAggregate(keys=[], functions=[avg(id#7)])
//  +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#15]
//  +- HashAggregate(keys=[], functions=[partial_avg(id#7)])
//  +- Exchange RoundRobinPartitioning(5), REPARTITION_BY_NUM, [id=#11]
//  +- LocalTableScan [id#7]

  //</editor-fold>

  //<editor-fold desc="Эксперимент 9">

  //  1. Делаем аггрегацию над уже перераспределенным датасетом
  //  Итог - снова выполняется перемешивание
  val experiment09DF = repartitionedById1
    .groupBy("salary")
    .agg(sum("id"))

//    experiment09DF.explain()

//  == Physical Plan ==
//    AdaptiveSparkPlan isFinalPlan=false
//  +- HashAggregate(keys=[salary#8], functions=[sum(id#7)])
//  +- Exchange hashpartitioning(salary#8, 200), ENSURE_REQUIREMENTS, [id=#15]
//  +- HashAggregate(keys=[salary#8], functions=[partial_sum(id#7)])
//  +- Exchange hashpartitioning(id#7, 200), REPARTITION_BY_COL, [id=#11]
//  +- LocalTableScan [id#7, salary#8]

  //</editor-fold>

  //<editor-fold desc="Выводы">

  // Перетасовка вызывается для перераспределения данных,
  // и она выполняется различных ситуациях.
  // Например, при выполнении аггрегирующих функций, группировках,
  // при некоторых сортировках, при соединениях.
  // Порядок обработки данных - до или после join - влияет на скорость.
  // Конвертация DF в DS - эксперимент 3 - влияет на скорость трансформации из-за операций десериализации/сериализации.
  // Операции, которые не требуют перераспределения, не вызывают перетасовку.
  // Это и добавление колонок, и их различные преобразования.

  //</editor-fold>

}
