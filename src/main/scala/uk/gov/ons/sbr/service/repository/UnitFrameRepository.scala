package uk.gov.ons.sbr.service.repository

import scala.util.Try

import org.apache.spark.sql.{DataFrame, SparkSession}

trait UnitFrameRepository {
  def retrieveTableAsDataFrame(unitFrameName: String)(implicit activeSession: SparkSession): Try[DataFrame]
  def updStartTableAsDataFrame(startTabName: String, SampleTabName: String )(implicit activeSession: SparkSession): DataFrame
  def saveDataFrameToTable(df:DataFrame, tableName:String)(implicit activeSession: SparkSession): Try[Unit]

}

object UnitFrameRepository {
  type ErrorMessage = String
}