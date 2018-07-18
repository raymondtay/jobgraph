package hicoden.jobgraph.fsm.runners

import hicoden.jobgraph.configuration.engine.model._
import hicoden.jobgraph.configuration.step.model._
import hicoden.jobgraph.cc.{HttpService, SchedulingAlgorithm, ClusterMetrics}
import hicoden.jobgraph.fsm.runners.runtime._

import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model._
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.util.{ByteString, Timeout}

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

  def genRestartPolicy = for {
    attempts ← choose(1, 5)
  } yield Restart(attempts)

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
    restartPol  ← genRestartPolicy
  } yield JobConfig(id, name, description, "target/scala-2.12", sessionid, restartPol, runner, Nil, Nil)

  def genPythonJobConfig = for {
    id          ← posNum[Int]
    name        ← alphaStr.suchThat(! _.isEmpty)
    description ← alphaStr.suchThat(! _.isEmpty)
    sessionid   ← alphaStr.suchThat(! _.isEmpty)
    runner      ← genPythonRunner
    restartPol  ← genRestartPolicy
  } yield JobConfig(id, name, description, "src/test/scripts", sessionid, restartPol, runner, Nil, Nil)

}


// In a unit test, its is NOT my desire to actually connect to a running
// Mesos Cluster (remember that unit tests are supposed to be not reliant on
// anything else "external")
object FakeLRU extends SchedulingAlgorithm {
  import cats._, data._, implicits._

  def run(fallback: List[MesosRuntimeConfig]) : Reader[Map[ClusterMetrics, MesosRuntimeConfig], MesosRuntimeConfig] =
    Reader{ (metricMap: Map[ClusterMetrics, MesosRuntimeConfig]) ⇒
      MesosRuntimeConfig(enabled = true, runas = "hicoden", hostname = "localhost", hostport = 5050) // this is the default value returned regardless of the metric map
    }
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

// Simulates an Mesos cluster serving the endpoint "/tasks.json"; that's how
// the Mesos cluster returns metric data which the LRU algorithm uses to decide
// which mesos cluster to select
// Note: The data returned here is based on "live" data in the google cloud
//       sandbox environment.
class FakeMesosHttpService extends HttpService {
  import cats.data.Kleisli, cats.implicits._
  import ContentTypes._
  import scala.concurrent.Future
  override def makeSingleRequest(implicit actorSystem : ActorSystem, actorMaterializer: ActorMaterializer) = Kleisli{
    (_uri: String) ⇒
      val jsonData = """
      {"tasks":[{"id":"00e84cee-a6b3-44bc-981c-c38da1d99301","name":"runSingleCommand task","framework_id":"5e007a1d-fce2-4630-ab26-edddec8218c1-0006","executor_id":"","slave_id":"8bfbcdfb-db90-4dcd-a035-129df5b00613-S1","state":"TASK_FINISHED","resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":1.0},"role":"*","statuses":[{"state":"TASK_STARTING","timestamp":1531196262.52504,"container_status":{"container_id":{"value":"7d8506a5-a090-47d4-a27e-5a0dcc1b5b88"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_RUNNING","timestamp":1531196262.54614,"container_status":{"container_id":{"value":"7d8506a5-a090-47d4-a27e-5a0dcc1b5b88"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_FINISHED","timestamp":1531196484.803,"container_status":{"container_id":{"value":"7d8506a5-a090-47d4-a27e-5a0dcc1b5b88"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_KILLED","timestamp":1531196561.64431}]},{"id":"dd7f430f-26dd-4f2b-8cee-f6d50def4878","name":"runSingleCommand task","framework_id":"5e007a1d-fce2-4630-ab26-edddec8218c1-0005","executor_id":"","slave_id":"8bfbcdfb-db90-4dcd-a035-129df5b00613-S0","state":"TASK_FINISHED","resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":1.0},"role":"*","statuses":[{"state":"TASK_STARTING","timestamp":1531191048.65996,"container_status":{"container_id":{"value":"ee72c34d-a4b2-47ca-bf16-a0622b3d8944"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.5"}]}]}},{"state":"TASK_RUNNING","timestamp":1531191048.67591,"container_status":{"container_id":{"value":"ee72c34d-a4b2-47ca-bf16-a0622b3d8944"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.5"}]}]}},{"state":"TASK_FINISHED","timestamp":1531191261.38711,"container_status":{"container_id":{"value":"ee72c34d-a4b2-47ca-bf16-a0622b3d8944"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.5"}]}]}},{"state":"TASK_KILLED","timestamp":1531191276.16946}]},{"id":"c7452ddf-909e-4bea-8290-f28fd12ea3c2","name":"runSingleCommand task","framework_id":"5e007a1d-fce2-4630-ab26-edddec8218c1-0004","executor_id":"","slave_id":"8bfbcdfb-db90-4dcd-a035-129df5b00613-S1","state":"TASK_KILLED","resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":1.0},"role":"*","statuses":[{"state":"TASK_STARTING","timestamp":1531191048.3285,"container_status":{"container_id":{"value":"d368ef8d-6c27-4091-bc82-25df154150c4"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_RUNNING","timestamp":1531191048.34818,"container_status":{"container_id":{"value":"d368ef8d-6c27-4091-bc82-25df154150c4"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_KILLED","timestamp":1531191276.06771}]},{"id":"f8fda365-085d-4ff2-b6ff-324bb8d70654","name":"runSingleCommand task","framework_id":"5e007a1d-fce2-4630-ab26-edddec8218c1-0003","executor_id":"","slave_id":"8bfbcdfb-db90-4dcd-a035-129df5b00613-S1","state":"TASK_FINISHED","resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":1.0},"role":"*","statuses":[{"state":"TASK_STARTING","timestamp":1531190748.40414,"container_status":{"container_id":{"value":"0f479101-8552-4e83-af9e-ea74216218bc"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_RUNNING","timestamp":1531190748.42607,"container_status":{"container_id":{"value":"0f479101-8552-4e83-af9e-ea74216218bc"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_FINISHED","timestamp":1531190965.69131,"container_status":{"container_id":{"value":"0f479101-8552-4e83-af9e-ea74216218bc"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_KILLED","timestamp":1531191047.3523}]},{"id":"1cb74198-0a05-4c5c-a7f8-967fbd866a5d","name":"runSingleCommand task","framework_id":"5e007a1d-fce2-4630-ab26-edddec8218c1-0002","executor_id":"","slave_id":"8bfbcdfb-db90-4dcd-a035-129df5b00613-S1","state":"TASK_FINISHED","resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":1.0},"role":"*","statuses":[{"state":"TASK_STARTING","timestamp":1530779176.24219,"container_status":{"container_id":{"value":"a1d0e079-e2d7-473e-94c6-bfb82272fa0e"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_RUNNING","timestamp":1530779176.27305,"container_status":{"container_id":{"value":"a1d0e079-e2d7-473e-94c6-bfb82272fa0e"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_FINISHED","timestamp":1530779403.69385,"container_status":{"container_id":{"value":"a1d0e079-e2d7-473e-94c6-bfb82272fa0e"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_KILLED","timestamp":1530779474.54291}]},{"id":"bccbc389-c400-4ecd-9a70-86445b06a940","name":"runSingleCommand task","framework_id":"5e007a1d-fce2-4630-ab26-edddec8218c1-0001","executor_id":"","slave_id":"8bfbcdfb-db90-4dcd-a035-129df5b00613-S1","state":"TASK_FINISHED","resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":1.0},"role":"*","statuses":[{"state":"TASK_STARTING","timestamp":1530779175.47742,"container_status":{"container_id":{"value":"37d5d525-75b9-419b-a807-94c88c3bba8f"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_RUNNING","timestamp":1530779175.49634,"container_status":{"container_id":{"value":"37d5d525-75b9-419b-a807-94c88c3bba8f"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_FINISHED","timestamp":1530779403.32527,"container_status":{"container_id":{"value":"37d5d525-75b9-419b-a807-94c88c3bba8f"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.6"}]}]}},{"state":"TASK_KILLED","timestamp":1530779474.20386}]},{"id":"dffc538f-e158-4180-b35f-813987bbf4e5","name":"runSingleCommand task","framework_id":"5e007a1d-fce2-4630-ab26-edddec8218c1-0000","executor_id":"","slave_id":"8bfbcdfb-db90-4dcd-a035-129df5b00613-S0","state":"TASK_KILLED","resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":1.0},"role":"*","statuses":[{"state":"TASK_STARTING","timestamp":1530778950.49591,"container_status":{"container_id":{"value":"0751f542-39b6-445a-95eb-454cb1651b9c"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.5"}]}]}},{"state":"TASK_RUNNING","timestamp":1530778950.51107,"container_status":{"container_id":{"value":"0751f542-39b6-445a-95eb-454cb1651b9c"},"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"10.148.0.5"}]}]}},{"state":"TASK_KILLED","timestamp":1530778984.94817}]}]}"""
    Future.successful(
      HttpResponse(entity = HttpEntity(`application/json`, jsonData))
    )
  }
}

class MesosDataflowRunnerSpecs(implicit ee : ExecutionEnv) extends Specification with ScalaCheck with runtime.JobContextManifest with AfterAll {
  override def is = sequential ^ s2"""
    Check that MesosDataflowRunner can actually trigger an java program $verifyCanRunJavaMesosDataflowRunnerAndReturn
    Check that MesosDataflowRunner can actually trigger an python program $verifyCanRunPythonMesosDataflowRunnerAndReturn
  """
  
  implicit val actorSystem = ActorSystem("MesosDataflowRunnerSpecsActorSystem")
  implicit val actorMaterializer = ActorMaterializer()

  def afterAll() = {
    actorSystem.terminate() 
    actorMaterializer.shutdown()
  }

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  def verifyCanRunJavaMesosDataflowRunnerAndReturn = {
    // Prepared dummy configuration for the unit-test
    val wfId      = java.util.UUID.randomUUID
    val jobId     = java.util.UUID.randomUUID
    val jobConfig = RunnerData.genJavaJobConfig.sample
    val mesosCfg  = MesosConfig(enabled = true, timeout = 10, runas = "hicoden", uris = "localhost:8080"::Nil)
    val ctx       = MesosExecContext(jobConfig.get, mesosCfg)
    val runner    = new MesosDataflowRunner
    val jobgraphCfg = JobgraphConfig("localhost", 8080)
    implicit val mesosRequestTimeout = 200.millis
    val fakeHttpService = new FakeMesosHttpService


    runner.run(ctx)(
      manifestMesos(wfId, jobId, mesosCfg, jobgraphCfg, fakeHttpService, select)(FakeLRU)
    ) must be_==(
      manifestMesos(wfId, jobId, mesosCfg, jobgraphCfg, fakeHttpService, select)(FakeLRU)(jobConfig.get)
    ).awaitFor(500.millis)
  }

  def verifyCanRunPythonMesosDataflowRunnerAndReturn = {
    // Prepared dummy configuration for the unit-test
    val wfId      = java.util.UUID.randomUUID
    val jobId     = java.util.UUID.randomUUID
    val jobConfig = RunnerData.genPythonJobConfig.sample
    val mesosCfg  = MesosConfig(enabled = true, timeout = 10, runas = "hicoden", uris = "localhost:5050"::Nil)
    val ctx       = MesosExecContext(jobConfig.get, mesosCfg)
    val runner    = new MesosDataflowRunner
    val jobgraphCfg = JobgraphConfig("localhost", 8080)
    implicit val mesosRequestTimeout = 200.millis
    val fakeHttpService = new FakeMesosHttpService


    runner.run(ctx)(
      manifestMesos(wfId, jobId, mesosCfg, jobgraphCfg, fakeHttpService, select)(FakeLRU)
    ) must be_==(
      manifestMesos(wfId, jobId, mesosCfg, jobgraphCfg, fakeHttpService, select)(FakeLRU)(jobConfig.get)
    ).awaitFor(500.millis)
  }

}
