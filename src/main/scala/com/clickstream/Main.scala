package com.clickstream

import com.clickstream.Сonstants.{BRAND_ID_COL, GOOD_ID_COL, USER_COL, catalog, logDir}
import com.clickstream.util.Utils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main extends App {
  implicit val spark = SparkSession.builder()
    .master("local")
    .appName("clickStreamAnalytics")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val partitions = getPartitionsList(logDir, 12)
  val catalogDf: DataFrame = spark.sparkContext.broadcast(catalog.toDF(GOOD_ID_COL, BRAND_ID_COL)).value
  val ratingCondition = 'rating <= 5

  lazy val logsDf = getLogsDf(partitions)
  lazy val usersBrandedDf = getUsersBrandedDf(logsDf, catalogDf)

  if (partitions.nonEmpty) {
    println("Logs dataframe:")
    logsDf.show(false) // ToDo - delete
    println("usersBrandedDf:")
    usersBrandedDf.show(false) // ToDo - delete

    task1()
    task2()
  } else
    println("Nothing to process")


  // ЗАДАНИЕ 1 (топ 5 брендов по числу визитов)
  private[this] def task1(): Unit = {
    val windowClicks = Window.partitionBy(USER_COL).orderBy('count.desc)

    val topByClick = usersBrandedDf
      .groupBy(USER_COL, BRAND_ID_COL).count()
      .withColumn("rating", row_number().over(windowClicks))
      .drop("count")
      .where(ratingCondition)

    println("topByClick Df:")
    topByClick.show(false) // ToDo - delete
  /*
  * Сохранение результатов топ-5 по кликам
  * */
  }



  // ЗАДАНИЕ 2 (топ 5 брендов по времени просмотра)
  private[this] def task2(): Unit = {
    val windowSumTime = Window.partitionBy(USER_COL).orderBy('timestamp.asc)
    val windowTimeRating = Window.partitionBy(USER_COL).orderBy(col("sum(watching_time)").asc)

    val topByTime = usersBrandedDf
      .withColumn("watching_time", lead('timestamp, 1).over(windowSumTime) - 'timestamp)
      .drop('timestamp)
      .groupBy(USER_COL, BRAND_ID_COL).sum("watching_time")
      .withColumn("rating", row_number().over(windowTimeRating))
      .where(ratingCondition)

    println("topByWatchTime Df:")
    topByTime.show(false) // ToDo - delete
    /*
    * Сохранение результатов топ-5 по времени
    * */
  }


}
