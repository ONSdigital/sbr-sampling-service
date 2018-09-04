package uk.gov.ons.sbr.helpers.utils

import java.io.File

import scala.io.Source.fromFile

import uk.gov.ons.sbr.utils.FileProcessor.Delimiter

object FileProcessorHelper {
  private val headerIndex = 1

  private def getLines(file: File): Iterator[String] = fromFile(file).getLines

  def lineAsListOfFields(file: File): List[List[String]] =
    getLines(file = file).map(_.split(Delimiter).toList).toList
}
