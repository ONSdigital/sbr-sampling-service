package uk.gov.ons.sbr.utils

import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.registers.Validation
import uk.gov.ons.sbr.service.repository.UnitFrameRepository.ErrorMessage

object HadoopPathProcessor {
  val Delimiter = ","
  val CSV = "csv"
  val Header = "header"

  def readCsvFileAsDataFrame(filePath: Path)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .read
      .option(Header, value = true)
      .csv(filePath.toString)

  def fromString(pathStr: String): Path =
    new Path(pathStr)

  def validatePath(pathStr: String): Validation[ErrorMessage, Path] =
    Validation.fromTry(Try(fromString(pathStr)), onFailure = _.getMessage)
}
