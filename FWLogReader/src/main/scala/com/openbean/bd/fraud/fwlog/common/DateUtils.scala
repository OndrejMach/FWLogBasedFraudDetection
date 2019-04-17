package com.openbean.bd.fraud.fwlog.common

import java.time.LocalDate
import java.time.temporal.ChronoUnit

object DateUtils {
  def getPartitionedPath(localDate: LocalDate, path: String) = {
      s"${path}/year=${localDate.getYear()}/"+
        s"month=${localDate.getMonthValue()}/" +
        s"day=${localDate.getDayOfMonth()}/"
  }

  def getPaths(from: LocalDate, to: LocalDate, path: String =""): Seq[String] = {
    for {i <- 0.toLong to ChronoUnit.DAYS.between(from, to)}
      yield getPartitionedPath(from.plusDays(i), path)
  }
}
