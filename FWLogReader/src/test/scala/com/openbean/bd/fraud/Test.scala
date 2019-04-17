package com.openbean.bd.fraud

import java.time.LocalDate

import com.openbean.bd.fraud.fwlog.common.DateUtils
import com.openbean.bd.fraud.fwlog.spark.{FWLogReader, FWLogReaderCSV}
import org.apache.spark.sql.SparkSession

object Test extends App {

  implicit val sparkSession = SparkSession.builder().appName("Test FWLog Reader").master("local[*]").getOrCreate()

  println(DateUtils.getPaths(LocalDate.now(),LocalDate.now().plusDays(3)).mkString(";"))

  val reader = new FWLogReaderCSV()
  val data = reader.getDataForToday("/Users/ondrej.machacek/data/FWLog/CCSFWLog/")

  data.printSchema()
  data.show()

}
