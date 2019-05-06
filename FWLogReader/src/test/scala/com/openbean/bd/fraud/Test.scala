package com.openbean.bd.fraud

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.openbean.bd.fraud.fwlog.common.DateUtils
import com.openbean.bd.fraud.fwlog.model.FWLogColumns
import com.openbean.bd.fraud.fwlog.spark.{CDRReader, CDRReaderParquet, EventWriter, EventWriterJSON, FWLogReaderCSV, ProcessFWLog, Processor, Reader}
import org.apache.spark.sql.SparkSession

object Test extends App {


  val DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")

  val from = LocalDateTime.parse("2019-05-03-00", DATE_FORMAT)
  val to = LocalDateTime.parse("2019-05-03-06", DATE_FORMAT)
  val path = "/Users/ondrej.machacek/data/actual/cdr.parquet/"
  val pathFWLog = "/Users/ondrej.machacek/data/FWLog/CCSFWLog"


  implicit val sparkSession = SparkSession.builder().appName("Test FWLog Reader").master("local[*]").getOrCreate()

  /*val cdrReader = new CDRReaderParquet()
  val fwLogReader = new FWLogReaderCSV()
  val resultWriter = new EventWriterJSON()

  val processor = new Processor(fwLogReader, cdrReader, resultWriter)
  println(from.toString + to.toString + path)
  processor.run(from, to, path, pathFWLog)*/




  //println(FWLogColumns.a_party.toString)

  //println(DateUtils.getPaths(LocalDate.now(),LocalDate.now().plusDays(3)).mkString(";"))

  val date = LocalDateTime.parse("2019-05-03-00", DATE_FORMAT)

  val reader : Reader = new FWLogReaderCSV()
  val data = reader.getData(date,"/Users/ondrej.machacek/data/actual/fwlogs/")

  data.printSchema()
  data.show()

  val filtered = ProcessFWLog.filterOriginCountryCode("420", data)
  filtered.show(false)

  val features = ProcessFWLog.getPreprocessed(filtered)
  data.printSchema()
  features.filter("count_records>1").show(false)

  //val writer = new EventWriterJSON()
  //writer.write(features, "/Users/ondrej.machacek/data/FWLog/features")



  sparkSession.stop()


}
