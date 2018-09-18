package uk.gov.ons.sbr.helpers.utils

import java.io.File
import java.nio.file.Path

import scala.io.Source.fromFile

import uk.gov.ons.sbr.utils.HadoopPathProcessor.Delimiter

object FileProcessor {
  private def getLines(file: File): Iterator[String] = fromFile(file).getLines

  def lineAsListOfFields(file: File): List[List[String]] =
    getLines(file = file).map(_.split(Delimiter).toList).toList

  def filterDirectory(aDirectory: Path, suffixPattern: String): List[File] =
    aDirectory.toFile.listFiles.filter(_.getName.endsWith(suffixPattern)).toList

//  /**
//    *
//    * @throws {FileNotFoundException} - given path does not exist
//    * @throws {IOException} - configuration cannot be set due to sparkSession not being active or setup up as requried
//    */
//  def filterDirectory(aDirectory: Path, suffixPattern: String)(implicit sparkSession: SparkSession): RemoteIterator[LocatedFileStatus] = {
//    val conf = sparkSession.sparkContext.hadoopConfiguration
//    val fileSystem = FileSystem.get(conf)
//    fileSystem.listFiles(aDirectory, false)
//  }
}
