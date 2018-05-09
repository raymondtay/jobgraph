package hicoden.jobgraph.configuration.workflow

import hicoden.jobgraph.configuration.workflow.model._

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, _}
import Gen.{containerOfN, choose, pick, mapOf, listOf, listOfN, oneOf}
import Prop.{forAll, throws, AnyOperators}

object WorkflowParserData {
  val validJobNs : List[String] = List("jobs", "jobs2", "jobs3")
  val validWfNs : List[String] = List("workflows", "workflows2")
  val invalidJobNs : List[String] = List("jobX", "jobsY", "jobsZ")

  def genValidJobNs : Gen[String] = oneOf(validJobNs)
  def genValidWfNs : Gen[String] = oneOf(validWfNs)
  def genInvalidJobNs : Gen[String] = oneOf(invalidJobNs)

  implicit val arbInvalidJobNamespaces = Arbitrary(genInvalidJobNs)
  implicit val arbValidJobNamespaces = Arbitrary(genValidJobNs)
  implicit val arbValidWfJobNamespaces = Arbitrary(genValidWfNs)
}

class WorkflowParserSpecs extends mutable.Specification with ScalaCheck with Parser {

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  {
    import WorkflowParserData.arbInvalidJobNamespaces
    "Invalid namespace keys to load the Workflow(s) configuration will result in 'ConfigReaderFailure' failure." >> prop { (ns: String) ⇒
      loadDefault(ns::Nil).toEither must beLeft((nel: NonEmptyList[HOCONWorkflowValidation]) ⇒ nel.head.errorMessage(ns) must be_==(s"Unable to load workflow configuration from namespace: $ns"))
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import WorkflowParserData.{arbValidWfJobNamespaces}
    "Valid namespace keys to load the workflow(s) configuration will result in success." >> prop { (ns: String) ⇒
      loadDefault(ns :: Nil).toEither must beRight((cfgs: List[WorkflowConfig]) ⇒ cfgs.size must beBetween(1,2))
      loadDefault(ns :: Nil).toList must not be empty
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

}
