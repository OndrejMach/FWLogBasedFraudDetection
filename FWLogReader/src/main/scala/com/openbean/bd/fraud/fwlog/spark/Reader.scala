package com.openbean.bd.fraud.fwlog.spark

import java.time.{LocalDateTime}

import com.openbean.bd.fraud.fwlog.common.Logger
import org.apache.spark.sql.DataFrame

trait Reader extends Logger {

  def getData(from: String, to: String, path: String): DataFrame

  def getData(from: LocalDateTime, to: LocalDateTime, path: String): DataFrame

  def getData(date: LocalDateTime, path: String): DataFrame

  def getData(date: String, path: String): DataFrame

  def getDataForToday(path: String): DataFrame
}


