package com.openbean.bd.fraud

import java.time.LocalDate

import com.openbean.bd.fraud.fwlog.common.DateUtils
import com.openbean.bd.fraud.fwlog.model.FWLogColumns
import com.openbean.bd.fraud.fwlog.spark.{CDRReaderParquet, FWLogReaderCSV, FWLogWriterJSON, ProcessFWLog, Reader}
import org.apache.spark.sql.SparkSession

object Test extends App {

  implicit val sparkSession = SparkSession.builder().appName("Test FWLog Reader").master("local[*]").getOrCreate()


  val reader = new CDRReaderParquet()

  val data = reader.getData("2019-02-02","/Users/ondrej.machacek/data/actual/cdr.parquet")

  data.printSchema()





  /*println(FWLogColumns.a_party.toString)

  println(DateUtils.getPaths(LocalDate.now(),LocalDate.now().plusDays(3)).mkString(";"))

  val reader : Reader = new FWLogReaderCSV()
  val data = reader.getData("2019-04-17","/Users/ondrej.machacek/data/FWLog/CCSFWLog/")

  data.printSchema()
  data.show()

  val filtered = ProcessFWLog.filterOriginCountryCode("420", data)
  filtered.show(false)

  val features = ProcessFWLog.getPreprocessed(filtered)
  features.show(false)

  val writer = new FWLogWriterJSON()
  writer.write(features, "/Users/ondrej.machacek/data/FWLog/features")

  */

  sparkSession.stop()


}
