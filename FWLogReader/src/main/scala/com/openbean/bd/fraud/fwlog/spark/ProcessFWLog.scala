package com.openbean.bd.fraud.fwlog.spark

import com.openbean.bd.fraud.fwlog.common.Logger
import com.openbean.bd.fraud.fwlog.model.FWLogColumns
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._



object ProcessFWLog extends Logger{
  case class HistogramEntry(hour: Int, allowed: Int, blocked: Int)
  case class FWLogEntryInterim(a_party: String, records: Array[HistogramEntry], count_records: Long)
  case class FWLogEntry(a_party: String, records: Map[Int, (Int, Int)])


  def filterOriginCountryCode(cc: String, input: DataFrame) : DataFrame = {
    input.filter(s"${FWLogColumns.a_party_countrycode.toString}=${cc}")
  }

  def translateToMap(fWLogEntry: FWLogEntryInterim) : FWLogEntry = {
    def getMap(array: Array[HistogramEntry]) : Map[Int, (Int,Int)] = {
      array.map(i => )
    }
  }

  def enrichWithDateData(input: DataFrame): DataFrame = {
    input
      .withColumn(FWLogColumns.timestamp_hour.toString, month(col("timestamp")))
      .withColumn(FWLogColumns.timestamp_day.toString, dayofmonth(col("timestamp")))
      .withColumn(FWLogColumns.timestamp_month.toString, month(col("timestamp")))
  }

  def getAggregated(input: DataFrame) : DataFrame = {
    input
      .groupBy(FWLogColumns.a_party.toString, FWLogColumns.status.toString, FWLogColumns.timestamp_hour.toString, FWLogColumns.timestamp_day.toString, FWLogColumns.timestamp_month.toString)
      .count()
  }

  def getSingleDataframe(allowed: DataFrame, blocked: DataFrame) : DataFrame = {
    val allowedRenamed = allowed.withColumnRenamed("count", FWLogColumns.count_allowed.toString).drop(FWLogColumns.status.toString)
    val blockedRenamed = blocked.withColumnRenamed("count", FWLogColumns.count_blocked.toString).drop(FWLogColumns.status.toString)

    allowedRenamed.join(blockedRenamed,
      allowedRenamed(FWLogColumns.a_party.toString) === blockedRenamed(FWLogColumns.a_party.toString) &&
        allowedRenamed(FWLogColumns.timestamp_month.toString) === blockedRenamed(FWLogColumns.timestamp_month.toString) &&
        allowedRenamed(FWLogColumns.timestamp_day.toString) === blockedRenamed(FWLogColumns.timestamp_day.toString) &&
        allowedRenamed(FWLogColumns.timestamp_hour.toString) === blockedRenamed(FWLogColumns.timestamp_hour.toString))
      .withColumn(FWLogColumns.records.toString, struct(allowedRenamed(FWLogColumns.timestamp_hour.toString),col(FWLogColumns.count_allowed.toString), col(FWLogColumns.count_blocked.toString ) ))
      .groupBy(allowedRenamed(FWLogColumns.a_party.toString)).agg(collect_list(FWLogColumns.records.toString).alias(FWLogColumns.records.toString), count(FWLogColumns.records.toString).alias(FWLogColumns.count_records.toString))
  }


  def getPreprocessed(input: DataFrame)(implicit sparkSession: SparkSession) :DataFrame = {
    import sparkSession.implicits._

    val withDateData = enrichWithDateData(input)

    val aggregated = getAggregated(withDateData)

    val result = getSingleDataframe(aggregated.filter(s"${FWLogColumns.status.toString}='ALLOWED'"), aggregated.filter(s"${FWLogColumns.status.toString}='BLOCKED'"))

    result.as[FWLogEntryInterim].map()


  }
}
