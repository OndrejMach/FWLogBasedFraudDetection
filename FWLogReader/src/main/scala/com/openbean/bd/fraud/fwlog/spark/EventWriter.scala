package com.openbean.bd.fraud.fwlog.spark

import com.openbean.bd.fraud.fwlog.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

trait EventWriter extends Logger {
  def write(data : DataFrame, basepath: String) : Unit
}

class EventWriterJSON(implicit sparkSession: SparkSession) extends EventWriter {
  override def write(data: DataFrame, basepath: String): Unit = {
    data
      .repartition(1)
      .write
      .json(path = basepath)
  }

}

class EventWriterParquet(implicit sparkSession: SparkSession) extends EventWriter{
  override def write(data: DataFrame, basepath: String): Unit = {
    data
      .repartition(1)
      .write
      .parquet(path = basepath)
  }
}
