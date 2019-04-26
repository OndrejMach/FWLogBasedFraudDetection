package com.openbean.bd.fraud.fwlog.spark

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.openbean.bd.fraud.fwlog.common.{DateUtils, Logger}
import com.openbean.bd.fraud.fwlog.model.FWLogColumns
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession, types}

trait FWLogReader extends Logger {
  val DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def getData(from: String, to: String, path: String): DataFrame

  def getData(from: LocalDate, to: LocalDate, path: String): DataFrame

  def getData(date: LocalDate, path: String): DataFrame

  def getData(date: String, path: String): DataFrame

  def getDataForToday(path: String): DataFrame
}

class FWLogReaderCSV(implicit sparkSession: SparkSession) extends FWLogReader  {
  val fwlogSchema = StructType(Array(
    StructField(FWLogColumns.timestamp.toString, TimestampType, true),
    StructField(FWLogColumns.a_party_countrycode.toString, StringType, true),
    StructField(FWLogColumns.a_party.toString, StringType, true),
    StructField(FWLogColumns.a_party_original_cc.toString, StringType, true),
    StructField(FWLogColumns.a_party_original.toString, StringType, true),
    StructField(FWLogColumns.b_party_countrycode.toString, StringType, true),
    StructField(FWLogColumns.b_party.toString, StringType, true),
    StructField(FWLogColumns.service_code.toString, StringType, true),
    StructField(FWLogColumns.status.toString, StringType, true),
    StructField(FWLogColumns.CNTDB_query_result.toString, StringType, true),
    StructField(FWLogColumns.overall_service_result.toString, StringType, true)
  ))

  override def getData(from: LocalDate, to: LocalDate, path: String): DataFrame =
    sparkSession.read
      .option("basePath", path)
      .option("delimiter", "|")
      .schema(fwlogSchema)
      .csv(DateUtils.getPaths(from, to): _*)

  override def getData(date: LocalDate, path: String): DataFrame =
    sparkSession.read
      .option("basePath", path)
      .option("delimiter", "|")
      .schema(fwlogSchema)
      .csv(DateUtils.getPartitionedPath(date, path))

  override def getData(dateString: String, path: String): DataFrame = getData(LocalDate.parse(dateString, DATE_FORMAT), path)

  override def getDataForToday(path: String): DataFrame = getData(LocalDate.now(), path)

  override def getData(from: String, to: String, path: String): DataFrame = getData(LocalDate.parse(from, DATE_FORMAT), LocalDate.parse(to, DATE_FORMAT), path)

}

class FWLogReaderParquet(implicit sparkSession: SparkSession) extends FWLogReader{
  override def getData(from: LocalDate, to: LocalDate, path: String): DataFrame = sparkSession.read.option("basePath", path).parquet(DateUtils.getPaths(from, to): _*)

  override def getData(date: LocalDate, path: String): DataFrame = sparkSession.read.option("basePath", path).parquet(DateUtils.getPartitionedPath(date, path))

  override def getData(date: String, path: String): DataFrame = getData(LocalDate.parse(date, DATE_FORMAT), path)

  override def getDataForToday(path: String): DataFrame = getData(LocalDate.now(), path)

  override def getData(from: String, to: String, path: String): DataFrame = getData(LocalDate.parse(from, DATE_FORMAT), LocalDate.parse(to, DATE_FORMAT), path)

}
