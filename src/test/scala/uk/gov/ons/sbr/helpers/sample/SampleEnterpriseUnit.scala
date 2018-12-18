package uk.gov.ons.sbr.helpers.sample

import uk.gov.ons.sbr.helpers.UnitFrameInputFixture

/**
  * SampleEnterpriseUnit
  * ----------------
  * Author: haqa
  * Date: 02 September 2018 - 19:02
  * Copyright (c) 2017  Office for National Statistics
  */
trait SampleEnterpriseUnit extends UnitFrameInputFixture

object SampleEnterpriseUnit {
  object FieldNames {
    val ern = "ern"
    val entref = "entref"
    val name = "name"
    val tradingStyle = "tradingstyle"
    val address1 = "address1"
    val address2 = "address2"
    val address3 = "address3"
    val address4 = "address4"
    val address5 = "address5"
    val postcode = "postcode"
    val legalStatus = "legalstatus"
    val sic07 = "sic07"
    val employees = "paye_empees"
    val jobs = "paye_jobs"
    val enterpriseTurnover = "ent_turnover"
    val standardTurnover = "std_turnover"
    val groupTurnover = "grp_turnover"
    val containedTurnover = "cntd_turnover"
    val apportionedTurnover = "app_turnover"
    val prn = "prn"
  }
}
