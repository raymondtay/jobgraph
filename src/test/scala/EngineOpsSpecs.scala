package hicoden.jobgraph.engine

import hicoden.jobgraph.{Job,JobId,WorkflowId}
import hicoden.jobgraph.configuration.step.{Parser ⇒ SParser, Loader ⇒ SLoader, StepLoaderData}
import hicoden.jobgraph.configuration.workflow._
import hicoden.jobgraph.configuration.step.model._
import hicoden.jobgraph.configuration.workflow.model._
import hicoden.jobgraph.configuration.step.JobDescriptorTable

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, arbInt ⇒ _ , _}
import Gen.{containerOfN, frequency,choose, pick, mapOf, listOf, listOfN, oneOf, numStr}
import Prop.{forAll, throws, AnyOperators}

object EngineOpsData {

  // Supporting workflow indices that stretches over a 64-bit number is enticing but
  // we shall take a more modest approach i.e. a 32-bit number is good enough
  // Another of saying it is that 64-bit values are considered invalid.

  // !!!NOTE!!!
  // Reason for the magic number '3' is because there are 3 workflow
  // configurations embedded into the configuration file used for the tests.
  def genValidWorkflowIndices = for {
    workflowIndices  ← Gen.posNum[Int]
  } yield scala.math.abs(workflowIndices) % 3

  def genNamespaces = for {
    wns ← listOfN(1, oneOf("workflows", "workflows2"))
  } yield (List("jobs","jobs2","jobs3"), wns)

  def genNamespaces2 = for {
    wns ← Gen.nonEmptyListOf(oneOf("workflows4", "workflows4566"))
  } yield (List("jobs","jobs2","jobs3"), wns)

  implicit val arbValidWorkflowIndices = Arbitrary(genValidWorkflowIndices)
  implicit val arbValidNamespaces = Arbitrary(genNamespaces)
  implicit val arbValidStepNamespacesNInvalidWfNamespace = Arbitrary(genNamespaces2)
}

class EngineOpsSpecs extends mutable.Specification with
                               ScalaCheck with
                               EngineOps {

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  // Depending on the passed-in parameter [[loadFalse]] then the function loads
  // different values for the purpose of testing.
  def loadConfigs(loadFalse : Boolean = true) = {
    object s extends SParser with SLoader
    object t extends Parser with Loader
    import quiver.{LNode, LEdge}

    val loadedJobConfigs = if (loadFalse) s.loadDefault("nonexistent" :: Nil) else s.loadDefault("jobs"::Nil)
    var jdt : JobDescriptorTable = scala.collection.immutable.HashMap.empty[Int, JobConfig]
    jdt = s.hydrateJobConfigs(loadedJobConfigs.toList.flatten).runS(jdt).value

    val loadedWfConfigs = if (loadFalse) t.loadDefault("nonexistent" :: Nil) else t.loadDefault("workflows" ::"workflows2"::"workflows3"::Nil)
    var wfdt : WorkflowDescriptorTable = scala.collection.immutable.HashMap.empty[Int, WorkflowConfig]
    wfdt = t.hydrateWorkflowConfigs(loadedWfConfigs.toList.flatten).runS(wfdt).value
    (jdt,wfdt)
  }

  {
    "Extracting non-existent workflows from the system is pure folly." >> prop{ (workflowIndex: Int) ⇒
      val (jdt, wfdt) = loadConfigs(loadFalse = true) // make this really explicit
      extractWorkflowConfigBy(workflowIndex)(jdt, wfdt) must beNone 
      jdt.size must be_==(0)
      wfdt.size must be_==(0)
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineOpsData.arbValidWorkflowIndices
    "Extracting existent workflows from the system is should return valid configurations." >> prop{ (workflowIndex: Int) ⇒
      val (jdt, wfdt) = loadConfigs(loadFalse = false)
      extractWorkflowConfigBy(workflowIndex)(jdt, wfdt) must beSome
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineOpsData.arbValidNamespaces
    "Parsing, validating and loading the steps and workflows configuration should be successful, if the configurations are valid" >> prop { (ns:(List[String], List[String])) ⇒

      val (jdt, wfdt) = prepareDescriptorTables(ns._1, ns._2)

      jdt.size must be_>(0)
      wfdt.size must be_>(0)
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineOpsData.arbValidStepNamespacesNInvalidWfNamespace
    "Parsing, validating and loading the steps should be successful but if the workflow namespace is invalid, then its not registered. " >> prop { (ns:(List[String], List[String])) ⇒
      val (jdt, wfdt) = prepareDescriptorTables(ns._1, ns._2)

      jdt.size must be_>(0)
      wfdt.size must be_==(0)

    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

}
