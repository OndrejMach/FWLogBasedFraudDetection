package com.openbean.bd.fraud.fwlog.spark

import java.time.{LocalDateTime}
import java.time.format.DateTimeFormatter

import com.openbean.bd.fraud.fwlog.common.DateUtils
import com.openbean.bd.fraud.fwlog.model.FWLogColumns
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

abstract class FWLogReader extends Reader {
  val DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
}


class FWLogReaderCSV(implicit sparkSession: SparkSession) extends FWLogReader {
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

  override def getData(from: LocalDateTime, to: LocalDateTime, path: String): DataFrame =
    sparkSession.read
      .option("basePath", path)
      .option("delimiter", "|")
      .schema(fwlogSchema)
      .csv(DateUtils.getPathsDay(from, to): _*)

  override def getData(date: LocalDateTime, path: String): DataFrame =
    sparkSession.read
      .option("basePath", path)
      .option("delimiter", "|")
      .schema(fwlogSchema)
      .csv(DateUtils.getPartitionedPathDay(date, path))

  override def getData(dateString: String, path: String): DataFrame = getData(LocalDateTime.parse(dateString, DATE_FORMAT), path)

  override def getDataForToday(path: String): DataFrame = getData(LocalDateTime.now(), path)

  override def getData(from: String, to: String, path: String): DataFrame = getData(LocalDateTime.parse(from, DATE_FORMAT), LocalDateTime.parse(to, DATE_FORMAT), path)

}

class FWLogReaderParquet(implicit sparkSession: SparkSession) extends FWLogReader {
  override def getData(from: LocalDateTime, to: LocalDateTime, path: String): DataFrame = sparkSession.read.option("basePath", path).parquet(DateUtils.getPathsDay(from, to): _*)

  override def getData(date: LocalDateTime, path: String): DataFrame = sparkSession.read.option("basePath", path).parquet(DateUtils.getPartitionedPathDay(date, path))

  override def getData(date: String, path: String): DataFrame = getData(LocalDateTime.parse(date, DATE_FORMAT), path)

  override def getDataForToday(path: String): DataFrame = getData(LocalDateTime.now(), path)

  override def getData(from: String, to: String, path: String): DataFrame = getData(LocalDateTime.parse(from, DATE_FORMAT), LocalDateTime.parse(to, DATE_FORMAT), path)

}