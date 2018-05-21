package hicoden.jobgraph.engine

import java.util.UUID

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{Route, HttpApp}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.headers._
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer

/**
 * 
 * JobCallbacks - The ReSTful interfaces to intercepting callbacks from executing jobs
 *
 * Supported APIs:
 *
 * /flow/<job-id>/job - this is used by the executing jobs to submit the Google Dataflow issued job-id
 *                      back to the engine.
 *
 * @author Raymond Tay
 * @version 1.0
 */
trait JobCallbacks {

  import cats._, data._, implicits._

  // [[actorSystem]] and [[actorMaterializer]] would be provided by the
  // implementor.
  // [[engine]] is the target of all operations listed here.
  //
  implicit val actorSystem : ActorSystem
  implicit val actorMaterializer : ActorMaterializer
  val engine : ActorRef

  //
  // Attempt to parse both strings as UUIDs. This would fail if either fails,
  // which makes sense because we cannot have one without the other
  // @param s workflow id
  // @param t job id
  //
  private[engine]
  def validateAsUUIDs : String ⇒ String ⇒ Either[Throwable, (UUID,UUID)] =
    (s: String) ⇒ (t: String) ⇒ scala.util.Try{ (UUID.fromString(s), UUID.fromString(t)) }.toEither

  //
  // Interpret the parsed results and inform the [[engine]] to start the
  // supervision of the job or not. In either case, we would return the
  // appropriate HTTP response
  // @param parsed result, see [[validateAsUUIDs]]
  // 
  private[engine]
  def tellEngineToSupervise =
    (parsedResult: Either[Throwable, (UUID,UUID)]) ⇒
      parsedResult.fold(
        failWith(_),
        {
          case ((wfId: UUID, jobId: UUID)) ⇒
            engine ! SuperviseJob(wfId, jobId)
            complete(s"OK. Engine will supervise Dataflow jobId: $jobId")
        })

  /**
    * The routing DSL will be embedded into the Engine when it starts up. 
    * See [[Engine]] for its usage.
    */
  val route : Route = 
    post { path("flow" / Segment / "job" / Segment)( (a, b) ⇒ (tellEngineToSupervise compose validateAsUUIDs(a))(b)) }

}


