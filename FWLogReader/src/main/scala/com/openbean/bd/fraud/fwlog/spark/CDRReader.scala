package com.openbean.bd.fraud.fwlog.spark
import java.time.{LocalDateTime}
import java.time.format.DateTimeFormatter

import com.openbean.bd.fraud.fwlog.common.DateUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class CDRReader extends Reader {
  val DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
  val OFFSET = 2

  def getDataHoursBack(hoursBack: Long, path:String) : DataFrame
}


class CDRReaderParquet(implicit sparkSession: SparkSession) extends CDRReader {
  override def getData(from: LocalDateTime, to: LocalDateTime, path: String): DataFrame = {
    if (!DateUtils.validDate(from) || !DateUtils.validDate(to) || !(DateUtils.validDateRange(from, to))){
      logger.error(s"Invalid dates ${from.toString}, ${to.toString}")
      sparkSession.emptyDataFrame
    } else {
      sparkSession.read.option("basePath", path).parquet(DateUtils.getPathsHour(from, to.minusHours(OFFSET)): _*)
    }
  }

  override def getData(date: LocalDateTime, path: String): DataFrame = {
    if (!DateUtils.validDate(date)) {
      logger.error(s"Invalid date ${date.toString}")
      sparkSession.emptyDataFrame
    } else {
      sparkSession.read.option("basePath", path).parquet(DateUtils.getPartitionedPathDay(date, path))
    }
  }

  override def getData(date: String, path: String): DataFrame = getData(LocalDateTime.parse(date, DATE_FORMAT), path)

  override def getDataForToday(path: String): DataFrame = getData(LocalDateTime.now(), path)

  override def getData(from: String, to: String, path: String): DataFrame = getData(LocalDateTime.parse(from, DATE_FORMAT), LocalDateTime.parse(to, DATE_FORMAT), path)

  override def getDataHoursBack(hoursBack: Long, path: String): DataFrame = getData(LocalDateTime.now().minusHours(hoursBack), LocalDateTime.now().minusHours(OFFSET), path)
}
