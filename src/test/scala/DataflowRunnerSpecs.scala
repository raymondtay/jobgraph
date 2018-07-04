package hicoden.jobgraph.fsm.runners

import hicoden.jobgraph.configuration.engine.model._
import hicoden.jobgraph.configuration.step.model._

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.specs2.specification.AfterAll
import org.specs2.concurrent.ExecutionEnv
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, _}
import Gen.{posNum, alphaStr, containerOfN, choose, pick, mapOf, listOf, listOfN, oneOf}
import Prop.{forAll, throws, AnyOperators}
import scala.concurrent._
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

object RunnerData {

  def genJavaModule = for {
    m ← oneOf(getClass.getClassLoader.getResource("gcloud_echo_job.sh").getPath.toString :: Nil)
  } yield m

  def genPythonModule = for {
    m ← oneOf("gcloud_echo_job" :: Nil)
  } yield m

  def genJavaRunnerType = for {
    m ← oneOf("Dataflow:java" :: Nil)
  } yield m

  def genPythonRunnerType = for {
    m ← oneOf("Dataflow:python" :: Nil)
  } yield m

  def genPythonCliArgs = for {
    m ← oneOf("--jobName test-0" ::
                 "--input gs://dummybucket/README.md" ::
                 "--output gs://dummybucket-test-1/XX" ::
                 "--runner DataflowRunner" ::
                 "--project hicoden" ::
                 "--temp_location gs://dummybucket-test-1/" :: Nil)
  } yield m

  def genJavaCliArgs = for {
    m ← oneOf("--jobName=test-0" ::
                 "--input=gs://dummybucket/README.md" ::
                 "--output=gs://dummybucket-test-1/XX" ::
                 "--runner=DataflowRunner" ::
                 "--project=hicoden" ::
                 "--temp_location=gs://dummybucket-test-1/" :: Nil)
  } yield m

  def genJavaRunner = for {
    module  ← genJavaModule
    runner  ← genJavaRunnerType
    cliargs ← genJavaCliArgs
  } yield Runner(module, runner, cliargs :: Nil)

  def genPythonRunner = for {
    module  ← genPythonModule
    runner  ← genPythonRunnerType
    cliargs ← genPythonCliArgs
  } yield Runner(module, runner, cliargs :: Nil)

  def genJavaJobConfig = for {
    id          ← posNum[Int]
    name        ← alphaStr.suchThat(! _.isEmpty)
    description ← alphaStr.suchThat(! _.isEmpty)
    sessionid   ← alphaStr.suchThat(! _.isEmpty)
    runner      ← genJavaRunner
  } yield JobConfig(id, name, description, "target/scala-2.12", sessionid, runner, Nil, Nil)

  def genPythonJobConfig = for {
    id          ← posNum[Int]
    name        ← alphaStr.suchThat(! _.isEmpty)
    description ← alphaStr.suchThat(! _.isEmpty)
    sessionid   ← alphaStr.suchThat(! _.isEmpty)
    runner      ← genPythonRunner
  } yield JobConfig(id, name, description, "src/test/scripts", sessionid, runner, Nil, Nil)

}

//
// ~~~~~ NOTE ~~~~~~~
//
// There are two test programs loaded into the classpath which can be
// discovered by the DataflowRunner when running these tests.
// For now, we are not storing the result of the run (you can check the test
// scripts which return a response) in order to check it here as the design
// might change.
//
class DataflowRunnerSpecs(implicit ee : ExecutionEnv) extends Specification with ScalaCheck with runtime.JobContextManifest with AfterAll {
  override def is = sequential ^ s2"""
    Check that DataflowRunner can actually trigger an java program $verifyCanRunJavaDataflowRunnerAndReturn
    Check that DataflowRunner can actually trigger an python program $verifyCanRunPythonDataflowRunnerAndReturn
  """
  
  implicit val actorSystem = ActorSystem("DataflowRunnerSpecsActorSystem")
  implicit val actorMaterializer = ActorMaterializer()

  def afterAll() = {
    actorSystem.terminate() 
    actorMaterializer.shutdown()
  }

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  def verifyCanRunJavaDataflowRunnerAndReturn = {
    val wfId = java.util.UUID.randomUUID
    val jobId = java.util.UUID.randomUUID
    val jobConfig = RunnerData.genJavaJobConfig.sample
    val ctx = ExecContext(jobConfig.get)
    val runner = new DataflowRunner
    val jobgraphCfg = JobgraphConfig("localhost", 8080)
    runner.run(ctx)(manifest(wfId, jobId, jobgraphCfg)) must be_==(manifest(wfId, jobId, jobgraphCfg)(jobConfig.get)).awaitFor(500.millis)
  }

  def verifyCanRunPythonDataflowRunnerAndReturn = {
    val wfId = java.util.UUID.randomUUID
    val jobId = java.util.UUID.randomUUID
    val jobConfig = RunnerData.genPythonJobConfig.sample
    val ctx = ExecContext(jobConfig.get)
    val runner = new DataflowRunner
    val jobgraphCfg = JobgraphConfig("localhost", 8080)
    runner.run(ctx)(manifest(wfId, jobId, jobgraphCfg)) must be_==(manifest(wfId, jobId, jobgraphCfg)(jobConfig.get)).awaitFor(500.millis)
  }

}
