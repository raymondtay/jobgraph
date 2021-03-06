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
import org.specs2.specification.BeforeEach
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
                             BeforeEach with
                             EngineOps {

  sequential // all specifications are run sequentially

  override def before {
    JDT  = JobDescriptors(scala.collection.immutable.HashMap.empty[Int, JobConfig])
    WFDT = WorkflowDescriptors(scala.collection.immutable.HashMap.empty[Int, WorkflowConfig])
  }
  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  // Depending on the passed-in parameter [[loadFalse]] then the function loads
  // different values for the purpose of testing.
  def loadConfigs(loadFalse : Boolean = true) = {
    object s extends SParser with SLoader
    object t extends Parser with Loader
    import quiver.{LNode, LEdge}

    val loadedJobConfigs = if (loadFalse) s.loadDefault("nonexistent" :: Nil) else s.loadDefault("jobs"::"jobs2"::"jobs3"::Nil)
    var jdt : JobDescriptorTable = scala.collection.immutable.HashMap.empty[Int, JobConfig]
    jdt = s.hydrateJobConfigs(loadedJobConfigs.toList.flatten).runS(jdt).value

    val loadedWfConfigs = if (loadFalse) t.loadDefault("nonexistent" :: Nil) else t.loadDefault("workflows" ::"workflows2"::"workflows3"::Nil)
    var wfdt : WorkflowDescriptorTable = scala.collection.immutable.HashMap.empty[Int, WorkflowConfig]
    wfdt = t.hydrateWorkflowConfigs(loadedWfConfigs.toList.flatten).runS(wfdt).value
    (JDT.copy(map = jdt),WFDT.copy(map = wfdt))
  }

  {
    "Extracting non-existent workflows from the system is pure folly." >> prop{ (workflowIndex: Int) ⇒
      val (jdt, wfdt) = loadConfigs(loadFalse = true) // make this really explicit
      extractWorkflowConfigBy(workflowIndex, None)(jdt, wfdt) must beNone 
      jdt.map.size must be_==(0)
      wfdt.map.size must be_==(0)
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineOpsData.arbValidWorkflowIndices
    "Extracting existent workflows from the system is should return valid configurations." >> prop{ (workflowIndex: Int) ⇒
      val (jdt, wfdt) = loadConfigs(loadFalse = false)
      extractWorkflowConfigBy(workflowIndex, None)(jdt, wfdt) must beSome
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineOpsData.arbValidWorkflowIndices
    "Overriding loaded configurations with 1 job overrides should be reflected accordingly." >> prop{ (workflowIndex: Int) ⇒
      import quiver._
      val (jdt, wfdt) = loadConfigs(loadFalse = false) // Load all configurations
      val overrde = JobOverrides(id = 1, description = "A really really really simple text".some, workdir = "/home/auser/directory".some, sessionid = "XOXP111".some, timeout = 9.some, runnerRunner = "/path/to/some/exec".some, runnerCliArgs = List("a", "b").some )
      val overrides = Some(JobConfigOverrides(overrides = List(overrde)))

      extractWorkflowConfigBy(workflowIndex, overrides)(jdt, wfdt) must beSome{ (p:(List[LNode[Job,JobId]], List[LEdge[Job,String]])) ⇒
        p._1.size must be_>(0)
        p._2.size must be_>(0)
        val config : JobConfig = p._1.filter(gNode ⇒ gNode.vertex.config.id == 1).head.vertex.config // find the overrided configuration in the JDT
        config.id must be_==(1)
        config.description.some   must be_==(overrde.description)
        config.workdir.some       must be_==(overrde.workdir)
        config.sessionid.some     must be_==(overrde.sessionid)
        config.runner.runner.some must be_==(overrde.runnerRunner)
      }
    }.set(minTestsOk = 1, workers = 1)
  }

  {
    import EngineOpsData.arbValidWorkflowIndices
    "Overriding loaded configurations with 2 job overrides should be reflected accordingly." >> prop{ (workflowIndex: Int) ⇒
      import quiver._
      val (jdt, wfdt) = loadConfigs(loadFalse = false) // Load all configurations
      val overrde =
        JobOverrides(id = 1, description = "A really really really simple text".some, workdir = "/home/auser/directory".some, sessionid = "XOXP111".some, timeout = 4.some, runnerRunner = "/path/to/some/exec".some, runnerCliArgs = List("a", "b").some )
      val overrde2 =
        JobOverrides(id = 2, description = "A really really really simple text with something".some, workdir = "/home/auser/directory/subdir".some, sessionid = "XOXP112".some, timeout = 9.some, runnerRunner = "/path/to/some/exec/file".some, runnerCliArgs = List("a", "b", "c").some )
      val overrides = Some(JobConfigOverrides(overrides = List(overrde, overrde2)))

      extractWorkflowConfigBy(workflowIndex, overrides)(jdt, wfdt) must beSome{ (p:(List[LNode[Job,JobId]], List[LEdge[Job,String]])) ⇒
        p._1.size must be_>(0)
        p._2.size must be_>(0)
        val config : JobConfig = p._1.filter(gNode ⇒ gNode.vertex.config.id == 1).head.vertex.config // find the overrided configuration in the JDT
        config.id must be_==(1)
        config.description.some   must be_==(overrde.description)
        config.workdir.some       must be_==(overrde.workdir)
        config.sessionid.some     must be_==(overrde.sessionid)
        config.runner.runner.some must be_==(overrde.runnerRunner)
        val config2 : JobConfig = p._1.filter(gNode ⇒ gNode.vertex.config.id == 2).head.vertex.config // find the overrided configuration in the JDT
        config2.id must be_==(2)
        config2.description.some   must be_==(overrde2.description)
        config2.workdir.some       must be_==(overrde2.workdir)
        config2.sessionid.some     must be_==(overrde2.sessionid)
        config2.runner.runner.some must be_==(overrde2.runnerRunner)
      }
    }.set(minTestsOk = 1, workers = 1)
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
