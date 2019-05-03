package com.openbean.bd.fraud.fwlog.common

import java.time.{LocalDateTime}
import java.time.temporal.ChronoUnit

object DateUtils {
  def getPartitionedPathDay(localDate: LocalDateTime, path: String) = {
      s"${path}/year=${localDate.getYear()}/"+
        s"month=${localDate.getMonthValue()}/" +
        s"day=${localDate.getDayOfMonth()}/"
  }

  def getPathsDay(from: LocalDateTime, to: LocalDateTime, path: String =""): Seq[String] = {
    for {i <- 0.toLong to ChronoUnit.DAYS.between(from, to)}
      yield getPartitionedPathDay(from.plusDays(i), path)
  }

  def getPartitionedPathHour(localDate: LocalDateTime, path: String) = {
    s"${path}/year=${localDate.getYear()}/"+
      s"month=${localDate.getMonthValue()}/" +
      s"day=${localDate.getDayOfMonth()}/" +
      s"hour=${localDate.getHour}/"
  }

  def getPathsHour(from: LocalDateTime, to: LocalDateTime, path: String =""): Seq[String] = {
    for {i <- 0.toLong to ChronoUnit.HOURS.between(from, to)}
      yield getPartitionedPathHour(from.plusHours(i), path)
  }

  def validDate(date: LocalDateTime) = {
    date.isBefore(LocalDateTime.now())
  }

  def validDateRange(from: LocalDateTime, to:LocalDateTime) = {
    to.isAfter(from) || to.isEqual(from)
  }
}
