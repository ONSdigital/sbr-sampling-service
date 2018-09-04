package uk.gov.ons.sbr.service

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}

import uk.gov.ons.sbr.helpers.TestSessionManager
import uk.gov.ons.sbr.service.validation.SampleMethodsArguments

class SamplingServiceMainSpec extends FreeSpec with Matchers with MockFactory{

  private trait Fixture {
    val aSparkSession = TestSessionManager.sparkSession
  }

  "A sample" - {
    "is created and exported" - {
      "when all arguments are valid and both methods are successful" ignore new Fixture {
        val input = SampleMethodsArguments(???, ???, ???)
        SamplingServiceMain.createSample(args = input)(aSparkSession)
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
