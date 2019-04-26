package com.openbean.bd.fraud.fwlog.spark

import com.openbean.bd.fraud.fwlog.common.Logger
import com.openbean.bd.fraud.fwlog.model.FWLogColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ProcessFWLog extends Logger{
  def filterOriginCountryCode(cc: String, input: DataFrame) : DataFrame = {
    input.filter(s"${FWLogColumns.a_party_countrycode.toString}=${cc}")
  }

  def getPreprocessed(input: DataFrame) :DataFrame = {
    input
      .withColumn(FWLogColumns.features.toString,
        struct(FWLogColumns.timestamp.toString,
          FWLogColumns.b_party_countrycode.toString,
          FWLogColumns.b_party.toString,
          FWLogColumns.status.toString))
      .groupBy(FWLogColumns.a_party.toString)
      .agg(collect_list(FWLogColumns.features.toString).alias(FWLogColumns.features.toString),
        count(FWLogColumns.features.toString).alias(FWLogColumns.count_calls.toString),
        max(FWLogColumns.status.toString).alias(FWLogColumns.fraud_label.toString))
  }
}
