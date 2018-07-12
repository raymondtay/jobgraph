package hicoden.jobgraph.engine

import hicoden.jobgraph.configuration.step.model.{JobConfig, Runner}

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
  * Web Services w.r.t Jobs and supports the following
  * (a) Creating a job
  * (b) Querying a job's static configuration
  * (c) Querying all job's static configuration
  *
  * @author Raymond Tay
  * @version 1.0
  */
trait JobWebServices {

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
  // of [[JobConfig]]
  //
  private[engine]
  def validateAsJobJson = Reader{ (data: akka.util.ByteString) ⇒
    import io.circe.generic.auto._, io.circe.syntax._
    parse(data.utf8String).getOrElse(Json.Null).as[JobConfig].toOption
  }

  //
  // Submit the new job to the engine for further validation and return
  // the response back to the requester. Note: The timeout value here is not
  // part of the configuration as its considered part of the internals.
  //
  private[engine]
  def submitNewJobToEngine : Reader[JobConfig, Option[Int]] = Reader{ (jobConfig : JobConfig) ⇒
    implicit val timeout = akka.util.Timeout(5 seconds)
    Await.result((engine ? ValidateJobSubmission(jobConfig)).mapTo[Option[Int]], timeout.duration)
  }

  //
  // Prepares the http response object by injecting the given job index
  // into the payload.
  //
  private
  def prepareGoodResponse = Reader{ (jobId: Int) ⇒
    import io.circe.syntax._
    HttpEntity(`application/json`, s"""{"job_index" : ${jobId}}""")
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
  def prepareJobListingResponse = Reader{ (jobConfigs: List[JobConfig]) ⇒
    import hicoden.jobgraph.engine.runtime.{jobConfigEncoder, runnerConfigEncoder}
    import io.circe.syntax._
    HttpEntity(`application/json`, jobConfigs.asJson.noSpaces)
  }

  /**
    * A central routes configuration which carries the handlers of the routes;
    * the following are supported:
    * (a) Creating a job (its really an attempt to create a job)
    * (b) Listing all job configurations
    *
    * @return Http-400 (either missing json or json with invalid format)
    *         Http-420 (if the json format does not conform or invalid after
    *         DSL validation)
    *         Http-200 (with the newly created job id)
    */
  val JobWebServicesRoutes : Route =
    post {
      path("jobs" / "create"){
        decodeRequest { (req:RequestContext) ⇒
          onComplete(req.request.entity.dataBytes.runFold(akka.util.ByteString(""))(_ ++ _)) {
            case Failure(error) ⇒ complete(StatusCodes.BadRequest, prepareBadResponse("Submitted payload is not valid."::Nil))
            case Success(data)  ⇒
              validateAsJobJson(data).fold(
                complete(StatusCodes.BadRequest, prepareBadResponse("Expecting a JSON payload in the request, none seen."::Nil))
              )(submitNewJobToEngine(_).fold(complete(StatusCodes.UnprocessableEntity, prepareBadResponse("JSON payload detected but invalid format."::Nil)))(job ⇒ complete(prepareGoodResponse(job))))
          }(req)
        }
      }
    } ~
    get {
      path("jobs" / PathEnd){
        implicit val listingJobTimeout : akka.util.Timeout = 5.seconds
        onComplete((engine ? JobListing).mapTo[List[JobConfig]]) {
          case Failure(error) ⇒ complete(StatusCodes.InternalServerError, prepareBadResponse("Engine unable to complete your request ; try again later." :: Nil)) 
          case Success(data)  ⇒ complete(prepareJobListingResponse(data)) 
        }
      }
    }

}


