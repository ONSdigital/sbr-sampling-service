package uk.gov.ons.sbr.helpers.utils

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Path

import uk.gov.ons.sbr.helpers.utils.DataTransformation.RawTable
import uk.gov.ons.sbr.utils.FileProcessor.Delimiter

object ExportTable {
  // Loan
  private def withWriter(file: File)(f: BufferedWriter => Unit): Unit = {
    val fileWriter = new FileWriter(file)
    try {
      val bufferedWriter = new BufferedWriter(fileWriter)
      try f(bufferedWriter)
      finally bufferedWriter.close()
    }
    finally fileWriter.close()
  }

  def apply(aListOfTableRows: RawTable, prefix: String): Path = {
    val testTempPath = TestFileUtils.createTempFile(prefix)
    withWriter(file = testTempPath.toFile){ writer =>
      aListOfTableRows.foreach { row =>
        writer.append(row.mkString(Delimiter))
        writer.newLine()
      }
    }
    testTempPath
  }
}
