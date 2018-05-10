package hicoden.jobgraph.configuration.workflow.internal

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
import Arbitrary.{arbString ⇒ _, _}
import Gen.{containerOfN, frequency,choose, pick, mapOf, listOf, listOfN, oneOf, numStr}
import Prop.{forAll, throws, AnyOperators}

object ConcretizerData {

  // Supporting job indices that stretches over a 64-bit number is enticing but
  // we shall take a more modest approach i.e. a 32-bit number is good enough
  // Another of saying it is that 64-bit values are considered invalid.
  def genInvalidForwardGraph = for {
    src  ← choose(0L, Long.MaxValue)
    dest ← choose(0L, Long.MaxValue)
  } yield s"$src -> $dest"

  def genInvalidBackwardGraph = for {
    src  <- choose(0L, Long.MaxValue)
    dest <- choose(0L, Long.MaxValue)
  } yield s"$dest <- $src"

  def genInvalidGraphs =for( g <- frequency( (1, genInvalidForwardGraph), (1,
    genInvalidBackwardGraph))) yield g

  def genForwardGraph = for {
    src  <- choose(0, Integer.MAX_VALUE)
    dest <- choose(0, Integer.MAX_VALUE)
  } yield s"${src} -> ${dest}"

  def genBackwardGraph = for {
    src  <- choose(0, Integer.MAX_VALUE)
    dest <- choose(0, Integer.MAX_VALUE)
  } yield s"${dest} <- ${src}"

  def genValidGraphs = for( g <- frequency( (1, genForwardGraph), (1,
    genBackwardGraph))) yield g

  implicit val arbInvalidGraphs = Arbitrary(genInvalidGraphs)
  implicit val arbValidGraphs = Arbitrary(genValidGraphs)
}

class ConcretizerSpecs extends mutable.Specification with
                               ScalaCheck with
                               Concretizer {

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  {
    import ConcretizerData.{arbInvalidGraphs}
    "Parsing DSL. Invalid jobgraphs are parsed and captured." >> prop { (graph: String) ⇒
      parseVertices(graph) match {
        case fastparse.core.Parsed.Success(_, _)    ⇒ false
        case fastparse.core.Parsed.Failure(_, _, _) ⇒ true /* expecting this */
      }
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import ConcretizerData.{arbValidGraphs}
    "Parsing DSL. Valid jobgraphs parsed and captured." >> prop { (graph: String) ⇒
      parseVertices(graph) match {
        case fastparse.core.Parsed.Success(_, _)    ⇒ true /* expecting this */
        case fastparse.core.Parsed.Failure(_, _, _) ⇒ false
      }
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import StepLoaderData.arbValidNamespaces
    "Parsing DSL. Collecting the vertices means we shall be generating the intermediate ADT for further graph processing, should be successful." >> prop { (ns: String) ⇒
      object s extends SParser with SLoader
      object t extends Parser with Loader
      val loadedJobConfigs = s.loadDefault("jobs" :: "jobs2" :: "jobs3" :: Nil) /* loads the configuration from the [[application.conf]] */
      loadedJobConfigs.toEither must beRight((cfgs: List[JobConfig]) ⇒ cfgs.size must be_==(4))
      loadedJobConfigs.toList must not be empty
      var jdt : JobDescriptorTable = scala.collection.immutable.HashMap.empty[Int, JobConfig]
      jdt = s.hydrateJobConfigs(loadedJobConfigs.toList.flatten).runS(jdt).value
      jdt.contains(loadedJobConfigs.toList.flatten.head.id) must beTrue

      val loadedWfConfigs = t.loadDefault("workflows" :: Nil) /* this loads the configuration from [[application.conf]] */
      loadedWfConfigs.toEither must beRight((cfgs: List[WorkflowConfig]) ⇒ cfgs.size must beBetween(1,2))
      loadedWfConfigs.toList must not be empty
      var wfdt : WorkflowDescriptorTable = scala.collection.immutable.HashMap.empty[Int, WorkflowConfig]
      wfdt = t.hydrateWorkflowConfigs(loadedWfConfigs.toList.flatten).runS(wfdt).value
      wfdt.contains(loadedWfConfigs.toList.flatten.head.id) must beTrue

      loadedWfConfigs.toList.flatten.map(wfConfig ⇒ collectVertices(wfConfig) must beRight( (fs: List[Forward]) ⇒ fs.size must be_==(4) ))

    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import StepLoaderData.arbValidNamespaces
    "Parsing DSL. Collecting the vertices means we shall be generating the intermediate ADT for further graph processing, should succeed when the searched vertices are ∈ in the pre-loaded steps." >> prop { (ns: String) ⇒
      object s extends SParser with SLoader
      object t extends Parser with Loader
      val loadedJobConfigs = s.loadDefault("jobs" :: "jobs2" :: "jobs3" :: Nil) /* loads the configuration from the [[application.conf]] */
      loadedJobConfigs.toEither must beRight((cfgs: List[JobConfig]) ⇒ cfgs.size must be_==(4))
      loadedJobConfigs.toList must not be empty
      var jdt : JobDescriptorTable = scala.collection.immutable.HashMap.empty[Int, JobConfig]
      jdt = s.hydrateJobConfigs(loadedJobConfigs.toList.flatten).runS(jdt).value
      jdt.contains(loadedJobConfigs.toList.flatten.head.id) must beTrue

      val loadedWfConfigs = t.loadDefault("workflows" :: Nil) /* this loads the configuration from [[application.conf]] */
      loadedWfConfigs.toEither must beRight((cfgs: List[WorkflowConfig]) ⇒ cfgs.size must beBetween(1,2))
      loadedWfConfigs.toList must not be empty
      var wfdt : WorkflowDescriptorTable = scala.collection.immutable.HashMap.empty[Int, WorkflowConfig]
      wfdt = t.hydrateWorkflowConfigs(loadedWfConfigs.toList.flatten).runS(wfdt).value
      wfdt.contains(loadedWfConfigs.toList.flatten.head.id) must beTrue

      loadedWfConfigs.toList.flatten.map(wfConfig ⇒ collectVertices(wfConfig)
        must beRight( (fs: List[Forward]) ⇒ verifyVerticesWith(jdt)(fs) must beRight ))

    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import StepLoaderData.arbValidNamespaces
    "Parsing DSL. Collecting the vertices means we shall be generating the intermediate ADT for further graph processing, should fail when the searched vertices are ∉ in the pre-loaded steps." >> prop { (ns: String) ⇒
      object s extends SParser with SLoader
      object t extends Parser with Loader
      val loadedJobConfigs = s.loadDefault("jobs" :: "jobs2" :: Nil) /* loads the configuration from the [[application.conf]] */
      loadedJobConfigs.toEither must beRight((cfgs: List[JobConfig]) ⇒ cfgs.size must be_==(2))
      loadedJobConfigs.toList must not be empty
      var jdt : JobDescriptorTable = scala.collection.immutable.HashMap.empty[Int, JobConfig]
      jdt = s.hydrateJobConfigs(loadedJobConfigs.toList.flatten).runS(jdt).value
      jdt.contains(loadedJobConfigs.toList.flatten.head.id) must beTrue

      val loadedWfConfigs = t.loadDefault("workflows" :: Nil) /* this loads the configuration from [[application.conf]] */
      loadedWfConfigs.toEither must beRight((cfgs: List[WorkflowConfig]) ⇒ cfgs.size must beBetween(1,2))
      loadedWfConfigs.toList must not be empty
      var wfdt : WorkflowDescriptorTable = scala.collection.immutable.HashMap.empty[Int, WorkflowConfig]
      wfdt = t.hydrateWorkflowConfigs(loadedWfConfigs.toList.flatten).runS(wfdt).value
      wfdt.contains(loadedWfConfigs.toList.flatten.head.id) must beTrue

      loadedWfConfigs.toList.flatten.map(wfConfig ⇒ collectVertices(wfConfig)
        must beRight( (fs: List[Forward]) ⇒ verifyVerticesWith(jdt)(fs) must beLeft ))

    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

}
