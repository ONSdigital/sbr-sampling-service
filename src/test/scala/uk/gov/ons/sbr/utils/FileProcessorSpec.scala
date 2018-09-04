package uk.gov.ons.sbr.utils

import java.io.FileWriter
import java.nio.file.{Path, Paths}

import org.scalatest.{FreeSpec, Matchers}

import uk.gov.ons.sbr.helpers.TestSessionManager
import uk.gov.ons.sbr.helpers.utils.TestFileUtils.createTempFile

class FileProcessorSpec extends FreeSpec with Matchers{

  private def writeLines(file: Path, text: String): Unit = {
    val fw = new FileWriter(file.toString, true)
    try {
      fw.write(text)
    }
    finally fw.close()
  }

  private trait Fixture {
    val aSparkSession = TestSessionManager.sparkSession
  }

  "A file is read" - {
    "when a valid path is given" - {
      "with a sparkSession" in new Fixture {
        val somePath = createTempFile(prefix = "example_string")
        writeLines(somePath, s"Id,Greeting\n1,Hello World\n2,Hallo Welt")
        FileProcessor.readCsvFileAsDataFrame(somePath)(aSparkSession).count shouldBe 2L
      }
    }
  }

  "A file path" - {
    "created from a path string" in new Fixture {
      val aPathString = "/tmp/test/file/example.csv"
      val expectedPath = Paths.get(aPathString)
      FileProcessor.fromString(aPathString) shouldBe expectedPath
    }
  }

}
