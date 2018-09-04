package uk.gov.ons.sbr.utils

import java.io.File
import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileProcessor {
  val Delimiter = ","
  val CSV = "csv"
  val Header = "header"

  def readCsvFileAsDataFrame(filePath: Path)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .read
      .option(Header, value = true)
      .csv(filePath.toString)

  def fromString(pathStr: String): Path =
    new File(pathStr).toPath

  def filterDirectory(aDirectory: Path, suffixPattern: String): List[File] =
    aDirectory.toFile.listFiles.filter(_.getName.endsWith(suffixPattern)).toList
}
