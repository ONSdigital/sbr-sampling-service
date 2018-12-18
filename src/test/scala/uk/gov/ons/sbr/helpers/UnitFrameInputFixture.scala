package uk.gov.ons.sbr.helpers

trait UnitFrameInputFixture{
  protected type TableName = String
  protected val aFrame = List
  protected val Properties = List

  protected val NoValue = ""

  protected def aFrameHeader(fieldNames: String*): List[String] = fieldNames.toList

  protected def aUnit(value: String*): List[String] = value.toList

  protected def aSelectionStrata(value: String*): List[String] = value.toList
}
