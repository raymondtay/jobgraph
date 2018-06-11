package hicoden.jobgraph.engine

import hicoden.jobgraph.configuration.workflow.model.WorkflowConfig

import org.specs2.mutable.Specification
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.Specs2RouteTest
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.server._
import org.scalacheck._
import Gen._
import Prop._
import Arbitrary._
import StatusCodes._
import Directives._

import scala.concurrent._
import scala.concurrent.duration._

//case class WorkflowConfig(id : Int, name : String, description : String, steps : List[Int], jobgraph : List[String])
object WorkflowDummyData {
  val notJson = oneOf("", "123123", "asdbasd12!@3213~@#~!")
  val jsonButNotWorkflowConfig = oneOf("{}", """{"hi": 42}""")
  val validWorkflowConfig = for {
    id          ← posNum[Int]
    name        ← alphaStr
    description ← alphaStr
    steps       ← listOfN(3, oneOf(1,2,3))
  } yield WorkflowConfig(id, name, description, steps, Nil)
}

class WorkflowWebServicesSpecs extends Specification with Specs2RouteTest with WorkflowWebServices {

  import cats._, data._, implicits._
  import io.circe._, io.circe.parser._

  val actorSystem = system
  val actorMaterializer = materializer

  // Higher combinator function that allows us to lift the http entity object
  // from the request (known at runtime) and converting it to a json object
  // using my favourite json parser i.e. Circe.
  //
  implicit val circeUnmarshaller = new Unmarshaller[HttpResponse, io.circe.Json] {
    import io.circe._, io.circe.parser._
    def apply(value: HttpResponse)(implicit ec: ExecutionContext, materializer: akka.stream.Materializer): Future[io.circe.Json] = Future {
      val d = Await.result(value.entity.dataBytes.runFold(akka.util.ByteString(""))(_ ++ _), 2 seconds)
      parse(d.utf8String).getOrElse(io.circe.Json.Null)
    }
  }

  // `engine` here loads all the jobs defined in the "jobs" namespaces
  val engine = system.actorOf(akka.actor.Props(classOf[Engine], "jobs"::Nil, Nil))

  "When submitting a ReST call to the Engine to create a workflow" should {
    import WorkflowDummyData._

    "return a HTTP-40x code when json payload is not detected." in {
      val data = notJson.sample.get
      Post(s"/flow/create", HttpEntity(`application/json`, data)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors": ["Expecting a JSON payload in the request, none seen."]}""").getOrElse(Json.Null)
      }
    }

    "return a HTTP-420 code when json payload is detected but not in a recognized format." in {
      val data = jsonButNotWorkflowConfig.sample.get
      Post(s"/flow/create", HttpEntity(`application/json`, data)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors": ["Expecting a JSON payload in the request, none seen."]}""").getOrElse(Json.Null)
      }
    }

    "return a HTTP-420 code when json payload is detected (workflow config is valid format) but Engine rejected it." in {
      import io.circe.generic.auto._, io.circe.syntax._
      val data = validWorkflowConfig.sample.get
      Post(s"/flow/create", HttpEntity(`application/json`, data.asJson.noSpaces)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual UnprocessableEntity
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors": ["JSON payload detected but invalid format."]}""").getOrElse(Json.Null)
      }
    }

    "return a HTTP-200 code when json payload is detected (workflow config is valid format) and Engine accepted it." in {
      import io.circe.generic.auto._, io.circe.syntax._
      val wfConfig =
        WorkflowConfig(id = 42,
                       name = "Test workflow name",
                       description = "Yeah, its a test",
                       steps = List(0, 1, 2),
                       jobgraph = List("0 -> 1", "1 -> 2")) // note: jobgraph is a DAG

      Post(s"/flow/create", HttpEntity(`application/json`, wfConfig.asJson.noSpaces)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"workflow_id": ${wfConfig.id}}""").getOrElse(Json.Null)
      }
    }

  }

}


