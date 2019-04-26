package com.openbean.bd.fraud.fwlog.spark

import com.openbean.bd.fraud.fwlog.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

trait FWLogWriter extends Logger {
  def write(data : DataFrame, basepath: String) : Unit
}

class FWLogWriterJSON(implicit sparkSession: SparkSession) extends FWLogWriter {
  override def write(data: DataFrame, basepath: String): Unit = {
    data
      .repartition(1)
      .write
      .json(path = basepath)
  }

}

class FWLogWriterParquet(implicit sparkSession: SparkSession) extends FWLogWriter{
  override def write(data: DataFrame, basepath: String): Unit = {
    data
      .repartition(1)
      .write
      .parquet(path = basepath)
  }
}
