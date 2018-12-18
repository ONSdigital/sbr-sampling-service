package uk.gov.ons.sbr.support

import scala.util.{Failure, Success}

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}

class TrySupportSpec extends FreeSpec with Matchers with MockFactory {
  private trait Fixture {
    val onFailure = mockFunction[Throwable, String]
    val onSuccess = mockFunction[Int, String]
  }

  "TrySupport" - {
    "when folding a Try" - {
      "applies the onFailure function to a Failure" in new Fixture {
        val cause = new Exception("Some error")
        onFailure.expects(cause).returning("Goodbye World!")

        TrySupport.fold(Failure(cause))(onFailure, onSuccess) shouldBe "Goodbye World!"
      }

      "applies the onSuccess function to a Success" in new Fixture {
        val someValue = 42
        onSuccess.expects(someValue).returning("Hello World!")

        TrySupport.fold(Success(someValue))(onFailure, onSuccess) shouldBe "Hello World!"
      }
    }

    "when converting a Try to an Either" - {
      "a Failure is a Left" in {
        val cause = new Exception("Some error")
        TrySupport.toEither(Failure(cause)) shouldBe Left(cause)
      }

      "a Success is a Right" in {
        TrySupport.toEither(Success("Hello World")) shouldBe Right("Hello World")
      }
    }
  }
}
