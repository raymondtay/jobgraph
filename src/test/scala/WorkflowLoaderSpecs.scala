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

object WorkflowLoaderData {
  val validNs : List[String] = List("workflows", "workflows2")
  val invalidNs : List[String] = List("workflowsX", "workflowsY", "workflowsZ")

  def genValidNs : Gen[String] = oneOf(validNs)
  def genInvalidNs : Gen[String] = oneOf(invalidNs)

  implicit val arbInvalidNamespaces = Arbitrary(genInvalidNs)
  implicit val arbValidNamespaces = Arbitrary(genValidNs)
}

class WorkflowLoaderSpecs extends mutable.Specification with ScalaCheck with Parser with Loader {

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  {
    import WorkflowLoaderData.arbInvalidNamespaces
    "Using invalid namespace keys to lookup the workflow(s) will result in failures." >> prop { (ns: String) ⇒
      val loadedConfigs = loadDefault(ns :: Nil)
      loadDefault(ns::Nil).toEither must beLeft((nel: NonEmptyList[HOCONWorkflowValidation]) ⇒ nel.head.errorMessage(ns) must be_==(s"Unable to load workflow configuration from namespace: $ns"))
      val jdt : WorkflowDescriptorTable = scala.collection.immutable.HashMap.empty[Int, WorkflowConfig]
      hydrateWorkflowConfigs(loadedConfigs.toList.flatten).runS(jdt).value.size must be_==(0)
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import WorkflowLoaderData.arbValidNamespaces
    "Using valid namespace keys to load the workflow(s) configuration will result in success." >> prop { (ns: String) ⇒
      val loadedConfigs = loadDefault(ns :: Nil)
      loadedConfigs.toEither must beRight((cfgs: List[WorkflowConfig]) ⇒ cfgs.size must beBetween(1,2))
      loadedConfigs.toList must not be empty
      val wfdt : WorkflowDescriptorTable = scala.collection.immutable.HashMap.empty[Int, WorkflowConfig]
      hydrateWorkflowConfigs(loadedConfigs.toList.flatten).runS(wfdt).value.size must beBetween(1, 2)
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import WorkflowLoaderData.arbValidNamespaces
    "Lookuping the WfDT with valid jobConfigs will result in success" >> prop { (ns: String) ⇒
      val loadedConfigs = loadDefault(ns :: Nil)
      loadedConfigs.toEither must beRight((cfgs: List[WorkflowConfig]) ⇒ cfgs.size must beBetween(1,2))
      loadedConfigs.toList must not be empty
      var wfdt : WorkflowDescriptorTable = scala.collection.immutable.HashMap.empty[Int, WorkflowConfig]
      wfdt = hydrateWorkflowConfigs(loadedConfigs.toList.flatten).runS(wfdt).value
      wfdt.contains(loadedConfigs.toList.flatten.head.id) must beTrue
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

}
