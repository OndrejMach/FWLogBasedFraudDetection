package com.openbean.bd.fraud.fwlog.model

import java.sql.Timestamp


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

object CDRColumns extends Enumeration {
  type dataIndices = Value
  val a_party_msisdn = Value(0)
  val b_party_msisdn = Value(1)
  val event_timestamp = Value(2)
  val record_type = Value(3)
  val call_type = Value(4)
  val orig_mcz_duration = Value(5)
  val cr_setup_duration_ten_ms = Value(6)
  val b_party_country_code = Value(7)
  val b_party_country_code_encoded = Value(8)
  val hour = Value(9)
  val minute = Value(10)
  val cr_basic_service_code = Value(11)
  val cr_basic_service_type = Value(12)
  val iaz_duration= Value(13)
  val cc_probability= Value(14)
}

case class CDR(a_party_msisdn :String,
               b_party_msisdn : String,
               event_timestamp : Timestamp,
               record_type: Int,
               call_type : Long,
               orig_mcz_duration : Int,
               cr_setup_duration_ten_ms : Int,
               b_party_country_code : String,
               b_party_country_code_encoded : String,
               hour : Int,
               minute : Int,
               cr_basic_service_code : String,
               cr_basic_service_type : String,
               iaz_duration: Int)
