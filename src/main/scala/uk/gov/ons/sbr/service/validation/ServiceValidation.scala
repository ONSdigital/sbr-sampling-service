package uk.gov.ons.sbr.service.validation

import java.nio.file.Path
import javax.inject.Singleton

import scala.util.Try

import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.registers.Validation.ErrorMessage
import uk.gov.ons.registers.{Failure, Success, Validation}
import uk.gov.ons.sbr.service.repository.UnitFrameRepository
import uk.gov.ons.sbr.service.repository.hive.HiveFrame
import uk.gov.ons.sbr.utils.FileProcessor

@Singleton
class ServiceValidation(repository: UnitFrameRepository) {
  private def checkNumberOfArgsGiven(serviceArgs: List[String]): (String, String, String, String) =
    serviceArgs match {
      case unitFrameDatabaseStr :: unitFrameTableNameStr :: stratificationPropertiesStr :: outputDirectoryStr :: Nil =>
        (unitFrameDatabaseStr, unitFrameTableNameStr, stratificationPropertiesStr, outputDirectoryStr)
      case _ =>
        throw new IllegalArgumentException(s"Failed to run Sampling job due to invalid number of arguments passed " +
          s"[$serviceArgs], expected (3) got [${serviceArgs.length}]")
    }

  private def checkProperties(implicit sparkSession: SparkSession): String => Validation[ErrorMessage, DataFrame] =
    (FileProcessor.fromString _).andThen( path =>
      Validation.fromTry(Try(FileProcessor.readCsvFileAsDataFrame(path)), onFailure = _.getMessage))

  private def checkUnitFrame(frameNameStr: String)(implicit sparkSession: SparkSession): Validation[ErrorMessage, DataFrame] =
    Validation.fromTry(repository.retrieveTableAsDataFrame(frameNameStr), onFailure =
      cause => throw new Exception(s"Failed to construct Hive Table to DataFrame", cause))

  private def checkOutputDirectory: String => Validation[ErrorMessage, Path] =
    (FileProcessor.fromString _).andThen( path =>
      if (path.toFile.isDirectory) Success(path)
      else Failure(s"Path does not resolve to an existing directory ${path.getFileName}")
    )

  def validateAndParseRuntimeArgs(args: List[String])(implicit sparkSession: SparkSession): SampleMethodsArguments = {
    // check length of list first
    val (unitFrameDatabaseStr, unitFrameTableNameStr, stratificationPropertiesStr, outputDirectoryStr) =
      checkNumberOfArgsGiven(args)

    val untiFrameStr = HiveFrame(database = unitFrameDatabaseStr, tableName = unitFrameTableNameStr)

    val formattedArgsOrError = Validation.map3(checkUnitFrame(untiFrameStr),
      checkProperties.apply(stratificationPropertiesStr), checkOutputDirectory(outputDirectoryStr)
    )(SampleMethodsArguments)

    Validation.fold(formattedArgsOrError)(
      errMsgs => throw new Exception(errMsgs.mkString(FileProcessor.Delimiter)), identity)
  }
}
