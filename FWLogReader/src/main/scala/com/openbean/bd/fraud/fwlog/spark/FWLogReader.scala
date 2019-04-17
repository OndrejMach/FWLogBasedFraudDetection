package com.openbean.bd.fraud.fwlog.spark

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.openbean.bd.fraud.fwlog.common.{DateUtils, Logger}
import org.apache.spark.sql._

trait FWLogReader {
  val DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def getData(from: String, to: String, path: String): DataFrame

  def getData(from: LocalDate, to: LocalDate, path: String): DataFrame

  def getData(date: LocalDate, path: String): DataFrame

  def detData(date: String, path: String): DataFrame

  def getDataForToday(path: String): DataFrame
}

class FWLogReaderParquet()(implicit sparkSession: SparkSession) extends FWLogReader with Logger {
  override def getData(from: LocalDate, to: LocalDate, path: String): DataFrame = sparkSession.read.option("basePath", path).parquet(DateUtils.getPaths(from, to): _*)

  override def getData(date: LocalDate, path: String): DataFrame = sparkSession.read.option("basePath", path).parquet(DateUtils.getPartitionedPath(date, path))

  override def detData(date: String, path: String): DataFrame = getData(LocalDate.parse(date, DATE_FORMAT), path)

  override def getDataForToday(path: String): DataFrame = getData(LocalDate.now(), path)

  override def getData(from: String, to: String, path: String): DataFrame = getData(LocalDate.parse(from, DATE_FORMAT), LocalDate.parse(to, DATE_FORMAT), path)

}

class FWLogReaderCSV()(implicit sparkSession: SparkSession) extends FWLogReader with Logger {
  override def getData(from: LocalDate, to: LocalDate, path: String): DataFrame =
    sparkSession.read
      .option("basePath", path)
      .option("delimiter", "|")
      .csv(DateUtils.getPaths(from, to): _*)

  override def getData(date: LocalDate, path: String): DataFrame =
    sparkSession.read
      .option("basePath", path)
      .option("delimiter", "|")
      .csv(DateUtils.getPartitionedPath(date, path))

  override def detData(date: String, path: String): DataFrame = getData(LocalDate.parse(date, DATE_FORMAT), path)

  override def getDataForToday(path: String): DataFrame = getData(LocalDate.now(), path)

  override def getData(from: String, to: String, path: String): DataFrame = getData(LocalDate.parse(from, DATE_FORMAT), LocalDate.parse(to, DATE_FORMAT), path)

}