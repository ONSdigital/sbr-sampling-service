package uk.gov.ons.sbr.service.repository

import scala.util.Try

import org.apache.spark.sql.{DataFrame, SparkSession}

trait UnitFrameRepository {
  def retrieveTableAsDataFrame(unitFrameName: String)(implicit activeSession: SparkSession): Try[DataFrame]
}

object UnitFrameRepository {
  type ErrorMessage = String
}