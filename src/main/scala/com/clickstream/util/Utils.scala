package com.clickstream.util

import com.clickstream.Сonstants.{GOOD_ID_COL, dateFormat, logDir}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.storage.StorageLevel

import java.io.File

object Utils {

  def getPartitionsList(dirName: String, month: Int): Array[String] = {
    new File(dirName)
      .listFiles
      .filter(n => n.isDirectory && getMonth(n.getName) == month)
      .map(_.getName)
  }


  private def getMonth(partName: String) = {
    val dtf = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
    val d = java.time.LocalDate.parse(partName, dtf)

    d.getMonthValue
  }

  def getLogsDf(partitions: Array[String])(implicit spark: SparkSession): DataFrame = {
    partitions.tail
      .foldLeft(csv2Df(partitions.head)) { (accDf, partition) =>
        accDf.union(csv2Df(partition))
      }
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  private[this] def csv2Df(partName: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("recursiveFileLookup", "true")
      .option("header", "true")
      .csv(logDir + partName)
  }

  def getUsersBrandedDf(logsDf: DataFrame, catalogDf: DataFrame)(implicit spark: SparkSession): DataFrame  = {
    import spark.implicits._

    logsDf
      .withColumn(GOOD_ID_COL, split('url, "lamoda.ru/", 2)(1))
      .drop('url)
      .join(catalogDf, Seq(GOOD_ID_COL), "left")
      .drop(col(GOOD_ID_COL))
      .withColumnRenamed("cookie", "user")
      .persist(StorageLevel.MEMORY_AND_DISK)
  }
}
