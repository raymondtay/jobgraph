package hicoden.jobgraph.cc

import hicoden.jobgraph.configuration.engine.model.MesosConfig
import hicoden.jobgraph.fsm.runners.runtime.MesosRuntimeConfig

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model._

import pureconfig._, syntax._
import org.specs2._
import org.specs2.specification.AfterAll
import org.scalacheck._
import Arbitrary._
import Gen._
import scala.concurrent.duration._

class ExistentMesos extends HttpService {
  import cats.data.Kleisli, cats.implicits._
  import ContentTypes._
  import scala.concurrent.Future
  override def makeSingleRequest(implicit actorSystem: ActorSystem, akkaMat: ActorMaterializer) = Kleisli{
    (_uri: String) ⇒
      val jsonData = """
      {"tasks":[
         {"id":"95dd62d1-e10c-4ed4-b88b-d94ab3af8b1d", "state" : "TASK_RUNNING"}
       ]}
      """
    Future.successful(
      HttpResponse(entity = HttpEntity(`application/json`, jsonData))
    )
  }
}


class SimulateTwoExistentMesos extends HttpService {
  import cats.data.Kleisli, cats.implicits._
  import ContentTypes._
  import scala.concurrent.Future
  var state = 0
  override def makeSingleRequest(implicit actorSystem: ActorSystem, akkaMat: ActorMaterializer) = Kleisli{
    (_uri: String) ⇒
      val jsonData =
      if (state == 0)
        """
        {"tasks":[
           {"id":"95dd62d1-e10c-4ed4-b88b-d94ab3af8b1d", "state" : "TASK_RUNNING"}
         ]}
        """
      else
        """
        {"tasks":[
           {"id":"95dd62d1-e10c-4ed4-b88b-d94ab3af8b1d", "state" : "TASK_RUNNING"}
           {"id":"99dd62d1-e10c-4ed4-b88b-d97ab3af8b1d", "state" : "TASK_RUNNING"}
         ]}
        """

    Future.successful(
      HttpResponse(entity = HttpEntity(`application/json`, jsonData))
    )
  }
}

object CCSchedulerData {
  import com.typesafe.config._

  val unreachableMesosCfg : List[Config] = List("""
    mesos {
      enabled  : false
      runas    : hicoden
      timeout  : 6
      uris     : ["10.148.0.4:5050", "10.148.0.7:5050"]
    }
    """).map(ConfigFactory.parseString(_))

  val oneReachableMesosCfg : List[Config] = List("""
    mesos {
      enabled  : true
      runas    : hicoden
      timeout  : 6
      uris     : ["localhost:5050"]
    }
    """).map(ConfigFactory.parseString(_))

  val twoReachableMesosCfg : List[Config] = List("""
    mesos {
      enabled  : true
      runas    : hicoden
      timeout  : 6
      uris     : ["localhost2:5050", "localhost1:5050"]
    }
    """).map(ConfigFactory.parseString(_))
}


/**
  * Compute Cluster Scheduling algorithm validation specs
  * @author Raymond Tay
  * @version 1.0
  * @date 04 July 2018
  */
class CCSchedulingSpecs extends Specification with StatsMiner with AfterAll {

  override def is = sequential ^ s2"""
    Check that when NO (i.e. zero) Apache Mesos compute cluster, is present; we cannot mine its data and LRU defaults to 'local' $whenNoCCIsPresentNLRUWorks
    Check that when at least 1 Apache Mesos compute cluster, is present; we can mine its data $whenOneCCIsPresent
    Check that when at least 2 Apache Mesos compute clusters, are present; we can mine its data and LRU picks the right one $whenTwoCCIsPresentNLRUWorks
  """

  def whenNoCCIsPresentNLRUWorks = {
    (loadConfig[MesosConfig](CCSchedulerData.unreachableMesosCfg.head.getConfig("mesos")) : @unchecked )match {
      case Right(cfg) ⇒
        implicit val timeout : Duration = cfg.timeout.seconds
        select(LRU)(timeout)(cfg) must be_==(MesosRuntimeConfig(false, "hicoden", "10.148.0.4", 5050))
    }
  }

  def whenOneCCIsPresent = {
    (loadConfig[MesosConfig](CCSchedulerData.oneReachableMesosCfg.head.getConfig("mesos")) : @unchecked )match {
      case Right(cfg) ⇒
        implicit val timeout : Duration = cfg.timeout.seconds
        select(LRU, new ExistentMesos)(timeout)(cfg) must be_==(MesosRuntimeConfig(true, "hicoden", "localhost", 5050))
    }
  }

  def whenTwoCCIsPresentNLRUWorks = {
    (loadConfig[MesosConfig](CCSchedulerData.twoReachableMesosCfg.head.getConfig("mesos")) : @unchecked )match {
      case Right(cfg) ⇒
        implicit val timeout : Duration = cfg.timeout.seconds
        select(LRU, new SimulateTwoExistentMesos)(timeout)(cfg) must be_==(MesosRuntimeConfig(true, "hicoden", "localhost1", 5050))
    }
  }

  implicit val actorSystem = ActorSystem("CCSchedulingSpecsActorSystem")
  implicit val actorMaterializer = ActorMaterializer()

  def afterAll() = {
    actorSystem.terminate() 
    actorMaterializer.shutdown()
  }

}
