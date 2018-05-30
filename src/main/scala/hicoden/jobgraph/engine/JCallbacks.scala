package hicoden.jobgraph.engine

import java.util.UUID

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{Route, RequestContext, HttpApp}
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
 *                      back to the engine. The payload accompanying that
 *                      request is a JSON structure
 *                      { "google_dataflow_id": "2018 blah blah blah" }
 *
 * @author Raymond Tay
 * @version 1.0
 */
trait JobCallbacks {

  import cats._, data._, implicits._
  import io.circe._, parser._

  // [[actorSystem]] and [[actorMaterializer]] would be provided by the
  // implementor.
  // [[engine]] is the target of all operations listed here.
  //
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val actorSystem : ActorSystem
  implicit val actorMaterializer : ActorMaterializer
  val engine : ActorRef


  /**
   * Attempt to parse both strings as UUIDs. This would fail if either fails,
   * which makes sense because we cannot have one without the other
   * @param s workflow id
   * @param t job id
   * 
   */
  private[engine]
  def validateAsUUIDs : String ⇒ String ⇒ Either[Throwable, (UUID,UUID)] =
    (s: String) ⇒ (t: String) ⇒ scala.util.Try{ (UUID.fromString(s), UUID.fromString(t)) }.toEither

  /**
   * Interpret the parsed results and inform the [[engine]] to start the
   * supervision of the job or not. In either case, we would return the
   * appropriate HTTP response
   * @param parsed result, see [[validateAsUUIDs]]
   */
  private[engine]
  def tellEngineToSupervise(entity: Option[io.circe.Json]) =
    (parsedResult: Either[Throwable, (UUID,UUID)]) ⇒
      parsedResult.fold(
        failWith(_),
        {
          case ((wfId: UUID, jobId: UUID)) ⇒
            entity.fold(complete("Request does not contain valid JSON")){
              data ⇒
                extractGoogleDataflowId(data).fold(complete("Either we did not see the key 'google_dataflow_id' or its in a format other than 'string'.")){
                  googleDataflowId ⇒
                    engine ! SuperviseJob(wfId, jobId, googleDataflowId)
                    complete(s"OK. Engine will supervise Dataflow jobId: $jobId")
                }
            }
        })

  private[engine]
  def extractGoogleDataflowId: Reader[io.circe.Json, Option[String]] = Reader{ (json: io.circe.Json) ⇒
    import io.circe.optics.JsonPath._
    val googleDataflowId = root.google_dataflow_id.string
    googleDataflowId.getOption(json)
  }

  /**
   * The routing DSL will be embedded into the Engine when it starts up. 
   * See [[Engine]] for its usage.
   */
  val route : Route = 
    post {
      path("flow" / Segment / "job" / Segment){ (a, b) ⇒
        decodeRequest { (req:RequestContext) ⇒
          onComplete(req.request.entity.dataBytes.runFold(akka.util.ByteString(""))(_ ++ _)) {
            case scala.util.Failure(error) ⇒ complete("Expecting a payload in the request, none seen.")
            case scala.util.Success(data)  ⇒ (tellEngineToSupervise(parse(data.utf8String).toOption) compose validateAsUUIDs(a))(b)
          }(req)
        }
      }
    }

}


