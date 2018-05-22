package hicoden.jobgraph.configuration.step

import hicoden.jobgraph.configuration.step.model._

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, _}
import Gen.{containerOfN, choose, pick, mapOf, listOf, listOfN, oneOf}
import Prop.{forAll, throws, AnyOperators}

object StepParserData {
  val validNs : List[String] = List("jobs", "jobs2", "jobs3")
  val validNs2 : List[String] = List("jobs4")
  val invalidNs : List[String] = List("jobX", "jobsY", "jobsZ")

  def genValidNs : Gen[String] = oneOf(validNs)
  def genValidNs2 : Gen[String] = oneOf(validNs2)
  def genInvalidNs : Gen[String] = oneOf(invalidNs)

  implicit val arbInvalidNamespaces = Arbitrary(genInvalidNs)
  implicit val arbValidNamespaces = Arbitrary(genValidNs)
  implicit val arbValidNamespaces2 = Arbitrary(genValidNs2)
}

class StepParserSpecs extends mutable.Specification with ScalaCheck with Parser {

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  {
    import StepParserData.arbInvalidNamespaces
    "Invalid namespace keys to load the step(s) configuration will result in 'ConfigReaderFailure' failure." >> prop { (ns: String) ⇒
      loadDefault(ns::Nil).toEither must beLeft((nel: NonEmptyList[HOCONValidation]) ⇒ nel.head.errorMessage must be_==(s"Unable to load configuration from namespace: $ns"))
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import StepParserData.arbValidNamespaces
    "Valid namespace keys to load the step(s) configuration will result in success." >> prop { (ns: String) ⇒
      loadDefault(ns :: Nil).toEither must beRight((cfgs: List[JobConfig]) ⇒ cfgs.size must beBetween(1,2))
      loadDefault(ns :: Nil).toList must not be empty
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import StepParserData.arbValidNamespaces2
    "Valid namespace keys to load the step(s) configuration (with invalid runner configs) will result in failures." >> prop { (ns: String) ⇒
      loadDefault(ns::Nil).toEither must beLeft{(nel: NonEmptyList[HOCONValidation]) ⇒
          nel.head.errorMessage must startWith("Allowed namespaces are")
          nel.tail.head.errorMessage must startWith("Unable to load")
      }
      loadDefault(ns :: Nil).toList must be empty
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

}
