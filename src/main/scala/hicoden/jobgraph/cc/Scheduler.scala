package hicoden.jobgraph.cc

import hicoden.jobgraph.configuration.engine.model.MesosConfig
import hicoden.jobgraph.fsm.runners.runtime.MesosRuntimeConfig

import akka.actor._
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.util.{ByteString, Timeout}

import akka.http.scaladsl.{HttpExt, Http}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{Route, RequestContext, HttpApp}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern._
import akka.stream.ActorMaterializer
import scala.language.postfixOps
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._
import java.util.UUID
import com.typesafe.scalalogging.Logger
import io.circe._, io.circe.parser._
import cats._, cats.free._, data._, implicits._

/**
  * Functions to support the scheduling of jobs on a per-compute cluster basis
  * The scheduling will look at all mesos clusters in its vicinity and lift the
  * metrics related to it and favour the compute cluster that is least busy.
  *
  * Note: there's more that can be done in terms of selection etc since we are
  *       at the stage of proof-of-concept.
  *
  * @author Raymond Tay
  * @version 1.0
  * @date 3 July 2018
  */

// Base trait where we consume a container of statistics and then decide how to
// make a selection via `run` which returns None or Some(mesos-server-config)
trait SchedulingAlgorithm {
  def run : Reader[Map[ClusterMetrics, MesosRuntimeConfig], MesosRuntimeConfig]
}

//
// From the compute cluster perspective, we have available a tonne of metrics
// quite literally to choose from and for this proof-of-concept i am going to
// choose to implement a LRU algorithm where we choose the compute cluster that
// is least busy as compared to the rest.
// Note:
// - This algoritm is invoked in lazy fashion in the sense that when jobs are
//   about to dispatched would this statistics be computed.
//
object LRU extends SchedulingAlgorithm {

  implicit val serverRanking = new Ordering[(MesosRuntimeConfig, Int)] {
    def compare(a: (MesosRuntimeConfig, Int), b: (MesosRuntimeConfig, Int)) = a._2 compare b._2
  }
  val busy_states = Set(FrameworkStates.TASK_RUNNING, FrameworkStates.TASK_STAGING, FrameworkStates.TASK_STARTING)

  def groupByCC(m: Map[ClusterMetrics, MesosRuntimeConfig]) = m.groupBy{ case (metric, server) ⇒ server }
  def detectBusyStates(xs: Set[ClusterMetrics]) = xs.filter(x ⇒ busy_states.contains(x.state))
  def leastBusyServer(m : Map[MesosRuntimeConfig, Set[ClusterMetrics]]) = {
    val mm = m.collect{ case (k, v) ⇒ (k, v.size) }
    val pq = collection.mutable.PriorityQueue.apply[(MesosRuntimeConfig, Int)](mm.toSeq:_*)
    pq.min._1.some
  }

  def run : Reader[Map[ClusterMetrics, MesosRuntimeConfig], MesosRuntimeConfig] =
    Reader{ (metricMap: Map[ClusterMetrics, MesosRuntimeConfig]) ⇒
      val sentinel = MesosRuntimeConfig(enabled = false, runas = "hicoden", hostname = "localhost", hostport = 5050)
      if (metricMap.isEmpty) sentinel else
        leastBusyServer(for { (server, metrics) ← groupByCC(metricMap) } yield (server, detectBusyStates(metrics.keySet))).fold(sentinel)(identity)
    }

}

trait StatsMiner { self ⇒

  import FrameworkStates.FrameworkState

  implicit val actorSystem : ActorSystem
  implicit val actorMaterializer : ActorMaterializer
  val logger = Logger(classOf[StatsMiner])

  /**
    * Selects the appropriate compute cluster to deliver the job
    * see [[manifestMesos]] to see the usage.
    * @param mesosCfg
    * @return MesosRuntimeConfig which tells us which apache mesos cluster to
    * run in or just run it locally.
    */
  def select(algo: SchedulingAlgorithm, httpService: HttpService = new RealHttpService)(implicit timeout : Duration) : Reader[MesosConfig, MesosRuntimeConfig] =
    Reader{ (mesosCfg: MesosConfig) ⇒
      val runtimeCfgs = mesosCfg.uris.map(uri ⇒ {
        val hp = uri.split(":")
        MesosRuntimeConfig(mesosCfg.enabled, mesosCfg.runas, hp(0), hp(1).toInt)
      })
      gatherStats(httpService)(timeout)(runtimeCfgs) match {
        case Left(error) ⇒
          logger.error(s"[Scheduler] Error in reading stats from compute cluster")
          algo.run(collection.immutable.HashMap.empty[ClusterMetrics, MesosRuntimeConfig])
        case Right(metrics) ⇒
          algo.run(metrics)
      }
    }

  /**
    * Iterates through the configurations and for each of them; make a ReST call
    * to "mesos-host:mesos-port/tasks.json" and retrieve all the task information
    * registering any errors we might encounter.
    * @param clusterCfg
    * @return Left(error) or Right(container of payloads)
    */
  def gatherStats(httpService: HttpService)(implicit timeout : Duration) : Reader[List[MesosRuntimeConfig], Either[Exception, Map[ClusterMetrics, MesosRuntimeConfig]]] =
    Reader{ (clusterCfgs: List[MesosRuntimeConfig]) ⇒
      val m = collection.mutable.HashMap.empty[ClusterMetrics, MesosRuntimeConfig]
      clusterCfgs.map( cCfg ⇒ gatherStat(httpService)(timeout)(cCfg).bifoldMap((e: Throwable) ⇒ logger.error(s"[Scheduler] Unable to retrieve statistics from : ${cCfg}"), {
        (cMetrics: List[ClusterMetrics]) ⇒
          cMetrics.map(c ⇒ m += (c → cCfg))
          logger.info(s"[Scheduler] Retrieved statistics from : ${cCfg}")
      }))
      m.toMap.asRight
    }

  /**
    * Make a ReST call to "mesos-host:mesos-port/tasks.json" and retrieve all
    * the task information; leverage akka-http connection pool settings to
    * actually make the call.
    *
    * See [Apache Mesos Tasks endpoint](http://mesos.apache.org/documentation/latest/endpoints/master/tasks.json/)
    *
    * @param clusterCfg
    * @return Left(error) or Right(payload)
    */
  def gatherStat(httpService: HttpService)(implicit timeout : Duration) : Reader[MesosRuntimeConfig, Either[Throwable, List[ClusterMetrics]]]=
    Reader{ (clusterCfg: MesosRuntimeConfig) ⇒
      import scala.concurrent.ExecutionContext.Implicits.global

      Try{
        httpService.makeSingleRequest.run(s"http://${clusterCfg.hostname}:${clusterCfg.hostport}/tasks.json")
      }.toOption.fold{
        logger.error(s"[Scheduler] Unable to contact compute cluster @ ${clusterCfg} via the contact point.")
        new Throwable(s"Compute cluster might be down; unable to contact it @ ${clusterCfg}").asLeft[List[ClusterMetrics]]
      }{
        (responseFut: Future[HttpResponse]) ⇒
          val result : Future[List[ClusterMetrics]] =
          responseFut.map{
            case response @ HttpResponse(StatusCodes.OK, _, entity, _) ⇒
              logger.info(s"[Scheduler] Contacted compute cluster @ ${clusterCfg} and retrieved the statistics.")
              Await.result(
                entity.dataBytes.runFold(akka.util.ByteString(""))(_ ++ _).map{ body ⇒
                  val stats : Json = parse(body.utf8String).getOrElse(Json.Null)
                  ccStatsToClusterMetrics(stats)
                }, timeout - (5.seconds))
            case other @ HttpResponse(_, _, _, _) ⇒
              other.discardEntityBytes()
              List(ClusterMetrics(null, FrameworkStates.TASK_UNKNOWN))// this sentinel represents an failure in the ReST request
          }
          Try{ Await.result(result, timeout) }.toEither
      }
    }

}


// Base trait for http based services
//
trait HttpService {
  import cats.data.Kleisli, cats.implicits._
  import scala.concurrent.Future
  def makeSingleRequest(implicit actorSystem : ActorSystem, actorMaterializer: ActorMaterializer) : Kleisli[Future,String,HttpResponse]
}

class RealHttpService extends HttpService {
  import cats.data.Kleisli, cats.implicits._
  override def makeSingleRequest(implicit actorSystem : ActorSystem, actorMaterializer: ActorMaterializer) = Kleisli{
    (_uri: String) ⇒
      Http().singleRequest(HttpRequest(uri = _uri))
  }
}

