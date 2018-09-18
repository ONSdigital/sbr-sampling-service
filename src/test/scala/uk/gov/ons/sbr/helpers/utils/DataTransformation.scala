package uk.gov.ons.sbr.helpers.utils

import java.io.File
import java.nio.file.Path
//import org.apache.hadoop.fs.Path

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import uk.gov.ons.sbr.utils.HadoopPathProcessor.CSV

object DataTransformation {
  private val HeaderIndex = 1

  type RawTable = Seq[Seq[String]]

  def toDataFrame(aListOfLines: RawTable)(implicit sparkSession: SparkSession): DataFrame = {
    val rows = aListOfLines.drop(HeaderIndex).map(Row.fromSeq)
    val rdd = sparkSession.sparkContext.makeRDD(rows)
    val fieldTypes = aListOfLines.head.map(StructField(_, dataType = StringType, nullable = false))
    sparkSession.createDataFrame(rdd, StructType(fieldTypes))
  }

  def getSampleFile(sampleOutputDir: Path): File = {
    val listOfCsvOutputFiles = FileProcessor.filterDirectory(sampleOutputDir, suffixPattern = s".$CSV")
    assert(listOfCsvOutputFiles.nonEmpty, message = s"found no files with extension [.$CSV] in [$sampleOutputDir] directory")
    listOfCsvOutputFiles.head
  }
}
