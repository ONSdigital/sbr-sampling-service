package uk.gov.ons.sbr.utils

import scala.io.Source

import org.apache.spark.sql.DataFrame
import org.scalatest.{FreeSpec, Matchers}

import uk.gov.ons.sbr.helpers.TestSessionManager
import uk.gov.ons.sbr.helpers.utils.TestFileUtils.createTempDirectory
import uk.gov.ons.sbr.utils.FileProcessor.CSV

class ExportSpec extends FreeSpec with Matchers{

  private trait Fixture {
    private val aSparkSession = TestSessionManager.sparkSession
    import aSparkSession.implicits._

    def aDataFrame(fields: Seq[(Int, String)], columnNames: String*): DataFrame =
      fields.toDF(columnNames:_*)

    val aTempDirectory = createTempDirectory(prefix = "export-unit-test-")
  }

  "A dataframe is exported" - {
    "when given a valid dataframe" - {
      "with an existing directory" in new Fixture {
        val someFields = Seq(
          (1, "Hello World"),
          (2, "Goodbye World!")
        )

        val someTestDf = aDataFrame(fields = someFields, columnNames = "rowId", "response")

        Export(someTestDf, aTempDirectory)
        val anOutput = FileProcessor.filterDirectory(aTempDirectory, suffixPattern = s".$CSV").head
        val fileContents = Source.fromFile(anOutput).getLines.toList
        val expectedOutput = someFields

        fileContents sameElements expectedOutput
      }
    }
  }

}
