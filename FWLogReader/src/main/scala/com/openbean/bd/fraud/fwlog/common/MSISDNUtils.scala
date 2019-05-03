package com.openbean.bd.fraud.fwlog.common

import com.openbean.bd.fraud.fwlog.model.CDR

object MSISDNUtils {
  def getCountryCodes(cdr: CDR) : CDR = {
    new CDR(a_party_msisdn =  cdr.a_party_msisdn,
      b_party_country_code_encoded = CountryDecoder.fromMSISDN(cdr.b_party_msisdn),
      b_party_msisdn = cdr.b_party_msisdn,
      event_timestamp =cdr.event_timestamp,
      record_type = cdr.record_type,
      call_type =cdr.call_type,
      orig_mcz_duration =cdr.orig_mcz_duration,
      cr_setup_duration_ten_ms= cdr.cr_setup_duration_ten_ms,
      b_party_country_code = cdr.b_party_country_code,
      hour = cdr.hour,
      minute =cdr.minute,
      cr_basic_service_code = cdr.cr_basic_service_code,
      cr_basic_service_type = cdr.cr_basic_service_type,
      iaz_duration= cdr.iaz_duration)
  }
  def validMSISDNs(cdr: CDR) = {
    (cdr.a_party_msisdn!= null) && (cdr.b_party_msisdn!=null)&& (cdr.a_party_msisdn.length>6)&& (cdr.b_party_msisdn.length>6) && cdr.a_party_msisdn.forall(_.isDigit) && cdr.b_party_msisdn.forall(_.isDigit)
  }

  def czechNumber(cdr: CDR) = {
    cdr.a_party_msisdn.startsWith("420") || cdr.a_party_msisdn.startsWith("+420") || cdr.a_party_msisdn.startsWith("00420")
  }
}
