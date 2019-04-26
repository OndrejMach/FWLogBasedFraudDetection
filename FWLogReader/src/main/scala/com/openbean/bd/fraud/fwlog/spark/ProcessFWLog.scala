package com.openbean.bd.fraud.fwlog.spark

import com.openbean.bd.fraud.fwlog.common.Logger
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

object ProcessFWLog extends Logger{
  def filterOriginCountryCode(cc: String, input: DataFrame) : DataFrame = {
    input.filter(s"a_party_countrycode=${cc}")
  }

  def getPreprocessed(input: DataFrame) :DataFrame = {
    input
      .withColumn("features", struct("timestamp", "b_party_countrycode", "b_party", "status"))
      .groupBy("a_party")
      .agg(collect_list("features").alias("features"),
        count("features").alias("count_calls"),
        max("status").alias("fraud_label"))
  }
}
