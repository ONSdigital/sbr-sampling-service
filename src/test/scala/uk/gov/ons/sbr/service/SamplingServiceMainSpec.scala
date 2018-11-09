package uk.gov.ons.sbr.service

import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}
import uk.gov.ons.sbr.SamplingTestData
import uk.gov.ons.sbr.helpers.TestSessionManager
import org.apache.hadoop.fs.Path
import uk.gov.ons.sbr.service.validation.SampleMethodArguments

class SamplingServiceMainSpec extends FreeSpec with Matchers with MockFactory with SamplingTestData{

  private trait Fixture {
    val aSparkSession = TestSessionManager.sparkSession
  }

  "A sample" - {
    "is created and exported" - {
      "when all arguments are valid and both methods are successful" ignore new Fixture {
        implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
        val input = SampleMethodArguments(List(dataDF, propsDF, new Path("Enterprise"),"paye_empees"))
        SamplingServiceMain.createSample(args = input)(aSparkSession)
        spark.stop()
      }
    }

    "is not created" - {
      "when Stratification method fails" ignore new Fixture {

      }

      "when Sampling method fails" ignore new Fixture {

      }
    }
  }
}
