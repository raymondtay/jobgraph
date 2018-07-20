package hicoden.jobgraph.engine

import hicoden.jobgraph.{JobStatus, WorkflowStatus, Workflow}
import hicoden.jobgraph.configuration.workflow.model.{WorkflowConfig, JobOverrides, JobConfigOverrides}

import java.util.UUID

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{Route, RequestContext, HttpApp}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern._
import akka.stream.ActorMaterializer
import scala.language.postfixOps
import scala.util._

/**
  * Web Services w.r.t Workflows and supports the following
  * (a) Creating a workflow
  * (b) Querying a workflow
  * (c) Updating a workflow
  *
  * @author Raymond Tay
  * @version 1.0
  */
trait WorkflowWebServices {

  import cats._, data._, implicits._
  import io.circe._, parser._

  // [[actorSystem]] and [[actorMaterializer]] would be provided by the
  // implementor.
  // [[engine]] is the target of all operations listed here.
  //
  import scala.concurrent._, duration._, ExecutionContext.Implicits.global

  implicit val actorSystem : ActorSystem
  implicit val actorMaterializer : ActorMaterializer
  val engine : ActorRef

  //
  // Validate the incoming payload can be reified as the actual representation
  // of [[WorkflowConfig]]
  //
  private[engine]
  def validateAsWorkflowJson = Reader{ (data: akka.util.ByteString) ⇒
    import io.circe.generic.auto._, io.circe.syntax._
    parse(data.utf8String).getOrElse(Json.Null).as[WorkflowConfig].toOption
  }

  //
  // Submit the new workflow to the engine for further validation and return
  // the response back to the requester. Note: The timeout value here is not
  // part of the configuration as its considered part of the internals.
  //
  private[engine]
  def submitNewWorkflowToEngine : Reader[WorkflowConfig, Option[Int]] = Reader{ (wfConfig : WorkflowConfig) ⇒
    implicit val timeout = akka.util.Timeout(5 seconds)
    Await.result((engine ? ValidateWorkflowSubmission(wfConfig)).mapTo[Option[Int]], timeout.duration)
  }

  //
  // Prepares the http response object by injecting the given workflow index
  // into the payload.
  //
  private
  def prepareGoodResponse = Reader{ (workflowId: Int) ⇒
    import io.circe.syntax._
    HttpEntity(`application/json`, s"""{"workflow_index" : ${workflowId}}""")
  }

  //
  // Prepares the http response object by injecting the given workflow id (i.e.
  // UUID format) into the payload.
  //
  private[engine]
  def prepareWorkflowResponse = Reader{ (workflowId: String) ⇒
    import io.circe.syntax._
    HttpEntity(`application/json`, s"""{"workflow_id" : ${workflowId.asJson}}""")
  }

  //
  // Prepares the http response object by injecting the given error messages
  // into the payload.
  //
  private
  def prepareBadResponse = Reader{ (messages: List[String]) ⇒
    import io.circe.syntax._
    HttpEntity(`application/json`, s"""{"errors" : ${messages.asJson}}""")
  }

  //
  // Prepares the http response and renders the appropriate JSON payload
  //
  private[engine]
  def prepareWorkflowStatusResponse = Reader{ (wfStatus: WorkflowStatus) ⇒
    import hicoden.jobgraph.engine.runtime.{jobStatusEncoder, wfStatusEncoder}
    import io.circe.syntax._
    HttpEntity(`application/json`, wfStatus.asJson.noSpaces)
  }

  //
  // Prepares the http response and renders the appropriate JSON payload
  //
  private[engine]
  def prepareWorkflowListingResponse = Reader{ (wfConfigs: List[WorkflowConfig]) ⇒
    import hicoden.jobgraph.engine.runtime.{workflowsEncoder, workflowCfgEncoder}
    import io.circe.syntax._
    HttpEntity(`application/json`, wfConfigs.asJson.noSpaces)
  }

  private[engine]
  def parseAsWorkflowIndex = Reader{ (s: String) ⇒ scala.util.Try{s.toInt}.toOption }

  private[engine]
  def extractJobOverrides = Reader{ (req: RequestContext) ⇒
    Either.catchNonFatal{ req.request.entity.dataBytes.runFold(akka.util.ByteString(""))(_ ++ _) }
  }

  def validateJobOverrides(implicit timeout: akka.util.Timeout) = Reader{ (data: String) ⇒
    import io.circe._, parser._, syntax._
    parse(data).getOrElse(Json.Null) match {
      case Json.Null ⇒ none
      case json ⇒
        Either.catchNonFatal{Await.result( (engine ? ValidateWorkflowJobOverrides(json)).mapTo[Option[Boolean]], timeout.duration)}.toOption.flatten
    }
  }

  // Capture the runtime exception and include the actual details of the
  // message in the response back to client.
  def whenFailureToExtractFromStream(req: RequestContext) =
    Reader{ (t: Throwable) ⇒
      complete(StatusCodes.InternalServerError, prepareBadResponse(s"Unable to extract data from Http stream - this is a biggy"::t.getMessage::Nil))(req)
    }

  // Extraction from the data stream is ok (take note that an empty string from
  // the data stream also means)
  def whenExtractionFromStreamOK(someIndex: String, req: RequestContext)(implicit timeout : akka.util.Timeout) : Reader[Future[akka.util.ByteString], Future[akka.http.scaladsl.server.RouteResult]] =
    Reader{ (d: Future[akka.util.ByteString]) ⇒
      val data = Await.result(d.mapTo[akka.util.ByteString], timeout.duration)

      // if the data is empty, its interpreted to mean that you want the
      // defaults and the validation of the workflow begins .. otherwise
      // we shall validate against the engine to determine whether
      // (a) JSON is valid format
      // (b) JSON properties is OK
      if (data.utf8String.isEmpty) validateStartWorkflowRequest(someIndex).run(req) else
      validateJobOverrides(timeout)(data.utf8String).fold(
        complete(StatusCodes.BadRequest, prepareBadResponse(s"Requested job overrides failed upon validation"::Nil))(req)
      )(result ⇒ {println(s"RESULT ----> $result");validateStartWorkflowRequest(someIndex).run(req)})
    }

  def validateStartWorkflowRequest(someIndex: String)(implicit timeout: akka.util.Timeout) =
    Reader { (req: RequestContext) ⇒
      parseAsWorkflowIndex(someIndex).fold(
        complete(StatusCodes.BadRequest, prepareBadResponse("A workflow index should be some number"::Nil))
      ){anIndex ⇒
        Await.result((engine ? StartWorkflow(anIndex)).mapTo[String], timeout.duration) match {
          case "No such id" ⇒ complete(StatusCodes.BadRequest, prepareBadResponse(s"No such workflow index/id found: $anIndex"::Nil))
          case workflowId ⇒ complete(prepareWorkflowResponse(workflowId))
        }
      }(req)
    }

  /**
    * A central routes configuration which carries the handlers of the routes;
    * the following are supported:
    * (a) Creating a workflow
    * (b) Starting a workflow
    *   (b.1) If you pass in a valid JSON payload to alter the runtime behavior
    *   of the job; then the job will be run with that
    *   (b.2) If you pass no other payload, then it would be interpreted to
    *   mean the jobs are run with their defaults, as installed.
    *   (b.3) If you pass a a valid JSON payload but fails the validation, then
    *   you will not be able to start the workflow at all.
    * (c) Listing all workflows
    * (d) Querying the status of the workflow in question
    *
    * @return Http-400 (either missing json or json with invalid format)
    *         Http-420 (if the json format does not conform or invalid after
    *         DSL validation)
    *         Http-200 (with the newly created workflow id)
    */
  val WorkflowWebServicesRoutes : Route =
    post {
      path("flows" / "create"){
        decodeRequest { (req:RequestContext) ⇒
          onComplete(req.request.entity.dataBytes.runFold(akka.util.ByteString(""))(_ ++ _)) {
            case Failure(error) ⇒ complete(StatusCodes.BadRequest, prepareBadResponse("Submitted payload is not valid."::Nil))
            case Success(data)  ⇒
              validateAsWorkflowJson(data).fold(
                complete(StatusCodes.BadRequest, prepareBadResponse("Expecting a JSON payload in the request, none seen."::Nil))
              )(submitNewWorkflowToEngine(_).fold(complete(StatusCodes.UnprocessableEntity, prepareBadResponse("JSON payload detected but invalid format."::Nil)))(workflow ⇒ complete(prepareGoodResponse(workflow))))
          }(req)
        }
      }
    } ~
    put {
      path("flows" / Segment / "start"){ someIndex ⇒
        decodeRequest{ (req: RequestContext) ⇒
          implicit val timeToExtractDataNValidateWithEngine : akka.util.Timeout = 6.seconds
          extractJobOverrides(req).fold(
            whenFailureToExtractFromStream(req).run,
            whenExtractionFromStreamOK(someIndex, req).run
          )
        }
      }
    } ~
    get {
      path("flows" / PathEnd){
        implicit val listingWfTimeout : akka.util.Timeout = 3.seconds
        onComplete((engine ? WorkflowListing).mapTo[List[WorkflowConfig]]) {
          case Failure(error) ⇒ complete(StatusCodes.InternalServerError, prepareBadResponse("Engine unable to complete your request ; try again later." :: Nil)) 
          case Success(data)  ⇒ complete(prepareWorkflowListingResponse(data)) 
        }
      } ~
      path("flows" / JavaUUID){ runtimeWorkflowId ⇒
        implicit val statusWfTimeout : akka.util.Timeout = 3.seconds
        onComplete((engine ? WorkflowRuntimeReport(runtimeWorkflowId)).mapTo[Option[WorkflowStatus]]) {
          case Failure(error) ⇒ complete(StatusCodes.InternalServerError, prepareBadResponse("Engine unable to complete your request ; try again later." :: Nil))
          case Success(data)  ⇒ data.fold(complete(StatusCodes.BadRequest, prepareBadResponse("Did not detect this workflow id in the system" :: Nil)))(data ⇒ complete(prepareWorkflowStatusResponse(data)))
        }
      }
    }

}


