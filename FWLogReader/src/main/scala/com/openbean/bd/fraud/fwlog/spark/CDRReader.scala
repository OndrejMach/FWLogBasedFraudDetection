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
      val paths = DateUtils.getPathsHour(from, to, path)
      logger.info(s"Reading paths ${paths.mkString(",")}")
      sparkSession.read.option("basePath", path).parquet(paths: _*)
    }
  }

  override def getData(date: LocalDateTime, path: String): DataFrame = {
    if (!DateUtils.validDate(date)) {
      logger.error(s"Invalid date ${date.toString}")
      sparkSession.emptyDataFrame
    } else {
      val partition = DateUtils.getPartitionedPathDay(date, path)
      logger.info(s"Reading path ${partition}")
      sparkSession.read.option("basePath", path).parquet(partition)
    }
  }

  override def getData(date: String, path: String): DataFrame = getData(LocalDateTime.parse(date, DATE_FORMAT), path)

  override def getDataForToday(path: String): DataFrame = getData(LocalDateTime.now(), path)

  override def getData(from: String, to: String, path: String): DataFrame = getData(LocalDateTime.parse(from, DATE_FORMAT), LocalDateTime.parse(to, DATE_FORMAT), path)

  override def getDataHoursBack(hoursBack: Long, path: String): DataFrame = getData(LocalDateTime.now().minusHours(hoursBack), LocalDateTime.now().minusHours(OFFSET), path)
}
