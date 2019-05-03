package com.openbean.bd.fraud.fwlog.spark

import java.time.LocalDateTime

import com.openbean.bd.fraud.fwlog.common.{CountryDecoder, Logger, MSISDNUtils}
import com.openbean.bd.fraud.fwlog.model.{CDR, CDRColumns}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

class Processor(fwLogReader: FWLogReader, cdrReader: CDRReader, eventWriter: EventWriter)(implicit sparkSession: SparkSession) extends Logger{


  def loadCDRs(from: LocalDateTime, to: LocalDateTime, path: String) = {
   cdrReader.getData(from,to, path)
    .select(
      CDRColumns.a_party_msisdn.toString(),
      CDRColumns.b_party_country_code.toString(),
      CDRColumns.b_party_country_code_encoded.toString(),
      CDRColumns.b_party_msisdn.toString(),
      CDRColumns.call_type.toString(),
      CDRColumns.cr_basic_service_code.toString(),
      CDRColumns.cr_basic_service_type.toString(),
      CDRColumns.event_timestamp.toString(),
      CDRColumns.hour.toString(),
      CDRColumns.orig_mcz_duration.toString(),
      CDRColumns.iaz_duration.toString,
      CDRColumns.cr_setup_duration_ten_ms.toString(),
      CDRColumns.minute.toString(),
      CDRColumns.record_type.toString()
    )
  }

  def getCCProbablity(input: Dataset[CDR]) : DataFrame = {
    val count = input.count()
    val probabilities = input.groupBy(CDRColumns.b_party_country_code_encoded.toString())
    .count().withColumn(CDRColumns.cc_probability.toString, col("count")/count).drop("count")
    input.join(probabilities, input.col(CDRColumns.b_party_country_code_encoded.toString()) === probabilities.col(CDRColumns.b_party_country_code_encoded.toString()),"leftouter")
      .drop(probabilities.col(CDRColumns.b_party_country_code_encoded.toString()))
  }

  def filterAndValidateCDRs(dataset: Dataset[CDR])(implicit sparkSession: SparkSession): Dataset[CDR] = {
    import sparkSession.implicits._

    dataset.filter(MSISDNUtils.validMSISDNs(_))
      .filter(MSISDNUtils.czechNumber(_))
      .map(MSISDNUtils.getCountryCodes(_))
  }

  def loadFWLogs(from: LocalDateTime, to: LocalDateTime, path: String) = {
    fwLogReader.getData(from, to, path)
  }


  def dataForRNN(input: DataFrame) : DataFrame = {
    input
      .withColumn("features",
        struct(CDRColumns.b_party_country_code_encoded.toString(),
          CDRColumns.b_party_msisdn.toString(),
          CDRColumns.call_type.toString(),
          CDRColumns.cr_basic_service_code.toString(),
          CDRColumns.cr_basic_service_type.toString(),
          CDRColumns.event_timestamp.toString(),
          CDRColumns.hour.toString(),
          CDRColumns.orig_mcz_duration.toString(),
          CDRColumns.iaz_duration.toString,
          CDRColumns.cr_setup_duration_ten_ms.toString(),
          CDRColumns.minute.toString(),
          CDRColumns.record_type.toString(),
          CDRColumns.cc_probability.toString
        ))
      /*
      .drop(CDRColumns.b_party_country_code_encoded.toString(),
        CDRColumns.b_party_msisdn.toString(),
        CDRColumns.call_type.toString(),
        CDRColumns.cr_basic_service_code.toString(),
        CDRColumns.cr_basic_service_type.toString(),
        CDRColumns.event_timestamp.toString(),
        CDRColumns.hour.toString(),
        CDRColumns.orig_mcz_duration.toString(),
        CDRColumns.iaz_duration.toString,
        CDRColumns.cr_setup_duration_ten_ms.toString(),
        CDRColumns.minute.toString(),
        CDRColumns.record_type.toString()
      )

       */
      .groupBy(CDRColumns.a_party_msisdn.toString())
      .agg(collect_list("features").alias("features"),count("features").alias("count"))
  }

  def run(from: LocalDateTime, to: LocalDateTime, pathCDR: String, pathFWLog: String) (implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    val cdrs = loadCDRs(from, to, pathCDR).as[CDR]

    val validatedCDRs = filterAndValidateCDRs(cdrs)

    val fwLog = loadFWLogs(from, to, pathFWLog)

    val withCCProbability = getCCProbablity(validatedCDRs)


    val aggregatesForRNN = dataForRNN(withCCProbability)

    eventWriter.write(aggregatesForRNN, "/Users/ondrej.machacek/tmp/RNNEvents")

    cdrs.printSchema()
    cdrs.show(truncate = false)

    aggregatesForRNN.printSchema()
    aggregatesForRNN.show(truncate = false)
  }

}
