package hicoden.jobgraph.engine

import hicoden.jobgraph.Workflow
import hicoden.jobgraph.configuration.workflow.model.WorkflowConfig

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
  private[engine]
  def prepareGoodResponse = Reader{ (workflowId: Int) ⇒ HttpEntity(`application/json`, s"""{"workflow_id" : ${workflowId}}""") }

  //
  // Prepares the http response object by injecting the given error messages
  // into the payload.
  //
  private[engine]
  def prepareBadResponse = Reader{ (messages: List[String]) ⇒
    import io.circe.syntax._
    HttpEntity(`application/json`, s"""{"errors" : ${messages.asJson}}""")
  }

  /**
    * Accepts a Json payload and validates whether the workflow can be created
    * @return Http-400 (either missing json or json with invalid format)
    *         Http-420 (if the json format does not conform or invalid after
    *         DSL validation)
    *         Http-200 (with the newly created workflow id)
    */
  val WorkflowWebServicesRoutes : Route =
    post {
      path("flow" / "create"){
        decodeRequest { (req:RequestContext) ⇒
          onComplete(req.request.entity.dataBytes.runFold(akka.util.ByteString(""))(_ ++ _)) {
            case scala.util.Failure(error) ⇒ complete(StatusCodes.BadRequest, prepareBadResponse("Submitted payload is not valid."::Nil))
            case scala.util.Success(data)  ⇒
              validateAsWorkflowJson(data).fold(
                complete(StatusCodes.BadRequest, prepareBadResponse("Expecting a JSON payload in the request, none seen."::Nil))
              )(submitNewWorkflowToEngine(_).fold(complete(StatusCodes.UnprocessableEntity, prepareBadResponse("JSON payload detected but invalid format."::Nil)))(workflow ⇒ complete(prepareGoodResponse(workflow))))
          }(req)
        }
      }
    }

}

