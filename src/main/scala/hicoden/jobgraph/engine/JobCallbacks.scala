package hicoden.jobgraph.engine

import java.util.UUID

import hicoden.jobgraph.JobStates
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
 * /flow/<jobgraph-workflow-id>/job/<jobgraph-job-id> HTTP POST
 *
 *                      - this is used by the executing jobs to submit the Google Dataflow issued job-id
 *                      back to the engine. The payload accompanying that
 *                      request is a JSON structure; non-compliance of the
 *                      payload structure means that nothing gets processed.
 *                      { "google_dataflow_id": "2018 blah blah blah" }
 *
 * /flow/<jobgraph-workflow-id>/cancel HTTP POST
 *
 *                      - this is to cancel the Google Dataflow issued job-id
 *                      or job-ids. If there is no json payload, it means to cancel the
 *                      current running workflow, if any.
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

  private[engine]
  def validateAsUUID : String ⇒ Either[Throwable, UUID] =
    (s: String) ⇒ scala.util.Try{ UUID.fromString(s) }.toEither

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
                Either.fromOption(extractGoogleDataflowId(data), extractNonGoogleDataflowJobStatus(data)).bimap(
                  jobStatus ⇒ {
                    jobStatus.fold(complete(s"Either we did not see the key 'google_dataflow_id' nor 'job_status' or its value is other than 'string' or an invalid job status.")){ reportedStatus ⇒
                    engine ! UpdateWorkflow(wfId, jobId, reportedStatus)
                    complete(s"OK. Engine has updated the job:[$jobId]'s status to: $reportedStatus")
                  }},
                  googleDataflowId ⇒ {
                    engine ! SuperviseJob(wfId, jobId, googleDataflowId)
                    complete(s"OK. Engine will supervise Dataflow jobId: $jobId")
                  }
               ).merge
            }
        })

  // Look for the key: "job_status" in the JSON payload and attempt to map the
  // response back; take note that if the payload contains any other value than
  // 'FAILED' or 'FINISHED' then 'UNKNOWN' will be registered.
  private[engine]
  def extractNonGoogleDataflowJobStatus : Reader[io.circe.Json, Option[JobStates.States]] = Reader{ (json: io.circe.Json) ⇒
    import io.circe.optics.JsonPath._
    import hicoden.jobgraph.engine.runtime.ApacheBeamJobStatusFunctions
    val jobStatus = root.job_status.string
    jobStatus.getOption(json).fold(none[JobStates.States])(purportedStatus ⇒ ApacheBeamJobStatusFunctions.lookup(purportedStatus))
  }

  // Look for the key: "google_dataflow_id" in the JSON payload.
  private[engine]
  def extractGoogleDataflowId: Reader[io.circe.Json, Option[String]] = Reader{ (json: io.circe.Json) ⇒
    import io.circe.optics.JsonPath._
    val googleDataflowId = root.google_dataflow_id.string
    val r = googleDataflowId.getOption(json)
    println(s"""
      given : ${json.noSpaces}
      so what did we find here: $r
      """)
    r
  }

  /**
   * Interpret the parsed results and inform the [[engine]] to cancel the
   * run of the Dataflow jobs. In either case, we would return the appropriate HTTP response
   * @param parsed result, see [[validateAsUUIDs]]
   */
  private[engine]
  def tellEngineToCancel =
    (parsedResult: Either[Throwable, UUID]) ⇒
      parsedResult.fold(
        failWith(_),
        {
          case (wfId: UUID) ⇒
            engine ! StopWorkflow(wfId)
            complete(s"OK. Engine will stop Dataflow for workflow-id: $wfId")
        })

  /**
   * The routing DSL will be embedded into the Engine when it starts up. 
   * See [[Engine]] for its usage.
   */
  val JobCallbackRoutes : Route = 
    post {
      path("flows" / Segment / "job" / Segment){ (a, b) ⇒
        decodeRequest { (req:RequestContext) ⇒
          onComplete(req.request.entity.dataBytes.runFold(akka.util.ByteString(""))(_ ++ _)) {
            case scala.util.Failure(error) ⇒ complete("Expecting a payload in the request, none seen.")
            case scala.util.Success(data)  ⇒ (tellEngineToSupervise(parse(data.utf8String).toOption) compose validateAsUUIDs(a))(b)
          }(req)
        }
      }
    } ~
    post {
      path("flows" / Segment / "cancel"){ a ⇒
        (tellEngineToCancel compose validateAsUUID)(a)
      }
    }

}


