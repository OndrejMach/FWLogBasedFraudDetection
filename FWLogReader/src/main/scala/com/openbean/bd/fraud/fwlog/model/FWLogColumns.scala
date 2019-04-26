package com.openbean.bd.fraud.fwlog.model

object FWLogColumns extends Enumeration {
  type dataIndices = Value
  val timestamp = Value(0)
  val a_party_countrycode= Value(1)
  val a_party= Value(2)
  val a_party_original_cc= Value(3)
  val a_party_original= Value(4)
  val b_party_countrycode= Value(5)
  val b_party= Value(6)
  val service_code= Value(7)
  val status= Value(8)
  val CNTDB_query_result= Value(9)
  val overall_service_result= Value(10)
  val features = Value(11)
  val count_calls = Value(12)
  val fraud_label = Value(13)
}
