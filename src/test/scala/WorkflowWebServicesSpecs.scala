package hicoden.jobgraph.engine

import hicoden.jobgraph.configuration.workflow.model.WorkflowConfig

import org.specs2.specification.{BeforeAfterEach, BeforeAfterAll}
import org.specs2.mutable.Specification
import akka.http.scaladsl.model.StatusCodes
import akka.testkit._ // for the 'dilated' method
import akka.http.scaladsl.testkit.{RouteTestTimeout, Specs2RouteTest}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.http.scaladsl.model.MediaTypes.{`text/plain`, `application/json`}
import akka.http.scaladsl.server._
import org.scalacheck._
import Gen._
import Prop._
import Arbitrary._
import StatusCodes._
import Directives._

import scala.language.postfixOps
import scala.concurrent._
import scala.concurrent.duration._

object WorkflowDummyData {
  val notJson = oneOf("", "123123", "asdbasd12!@3213~@#~!")
  val jsonButNotWorkflowConfig = oneOf("{}", """{"hi": 42}""")
  val validWorkflowConfig = for {
    id          ← posNum[Int]
    name        ← alphaStr
    description ← alphaStr
  } yield WorkflowConfig(id, name, description, Nil)
}


trait WorkflowSpecsFunctions {
  
  import cats._, data._, implicits._
  import io.circe._, io.circe.parser._

  // Higher combinator function that allows us to lift the http entity object
  // from the request (known at runtime) and converting it to a json object
  // using my favourite json parser i.e. Circe.
  //
  implicit val circeUnmarshaller = new Unmarshaller[HttpResponse, io.circe.Json] {
    import io.circe._, io.circe.parser._
    def apply(value: HttpResponse)(implicit ec: ExecutionContext, materializer: akka.stream.Materializer): Future[io.circe.Json] = Future {
      val d = Await.result(value.entity.dataBytes.runFold(akka.util.ByteString(""))(_ ++ _), 1 seconds)
      parse(d.utf8String).getOrElse(io.circe.Json.Null)
    }
  }

  // Use lenses to inspect whether the returned ADT contains the key i'm
  // looking for.
  private[engine]
  def verifyWorkflowIdEmbedded = Reader{ (json: io.circe.Json) ⇒
    import io.circe.optics.JsonPath._
    val f = root.workflow_id.string
    f.getOption(json) match {
      case None    ⇒ (false, null)
      case Some(id) ⇒ (true, id)
    }
  }

  // Use lenses to inspect whether the returned ADT contains the key i'm
  // looking for; in this case i'm looking for an array w/o inspecting the
  // elements
  private[engine]
  def verifyWorkflowsEmbedded = Reader{ (json: io.circe.Json) ⇒
    import io.circe.optics.JsonPath._
    val f = root.workflows.arr
    f.getOption(json) match {
      case None    ⇒ false
      case Some(_) ⇒ true
    }
  }

  private[engine]
  def verifyWorkflowReportEmbedded = Reader{ (json: io.circe.Json) ⇒
    import io.circe.optics.JsonPath._
    val f = root.createTime.string
    val g = root.status.string
    val h = root.steps.arr
    def constraintsSatisfied(a: String, b: String, c: Vector[io.circe.Json]) =
      !(a.isEmpty) && !(b.isEmpty) && (c.size > 0)

    (
      f.getOption(json),
      g.getOption(json),
      h.getOption(json)
    ).mapN(constraintsSatisfied)
  }

}

class WorkflowWebServicesSpecs extends Specification with Specs2RouteTest with WorkflowWebServices with WorkflowSpecsFunctions with BeforeAfterAll {

  import cats._, data._, implicits._
  import io.circe._, io.circe.parser._

  val actorSystem = system
  val actorMaterializer = materializer

  override def beforeAll() = {}
  override def afterAll() = {
    actorMaterializer.shutdown()
    actorSystem.terminate()
  }

  sequential
  implicit val routeTimeout = RouteTestTimeout(3.seconds.dilated)

  // `engine` here loads all the jobs defined in the "jobs" namespaces, but
  // notice that no workflows are loaded statically.
  val engine = system.actorOf(akka.actor.Props(classOf[Engine], None, "jobs"::"jobs2"::"jobs3"::Nil, Nil))

  "When querying for workflow(s) in the system" in {

    "return a HTTP-420 code when the workflows does not exist in the system." in {
      Get(s"/flows/") ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        verifyWorkflowsEmbedded.run(responseAs[io.circe.Json])
      }
    }

    "return a HTTP-420 code when any workflow does not exist in the system; regardless of the fact the Uri-format request is valid." in {
      Get(s"/flows/${java.util.UUID.randomUUID}") ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
        verifyWorkflowReportEmbedded.run(responseAs[io.circe.Json]) must beNone
      }
    }

  }

}

class WorkflowWebServicesSpecs2 extends Specification with Specs2RouteTest with WorkflowWebServices with WorkflowSpecsFunctions with BeforeAfterAll with BeforeAfterEach {
  import cats._, data._, implicits._
  import io.circe._, io.circe.parser._

  val actorSystem = system
  val actorMaterializer = materializer

  override def before() = {
    persistence.DbFunctions.clearJobsRuntimeData(persistence.Transactors.xa)
  }
  override def after() = {}
  override def beforeAll() = {}
  override def afterAll() = {
    actorMaterializer.shutdown()
    actorSystem.terminate()
  }

  sequential
  implicit val routeTimeout = RouteTestTimeout(3.seconds.dilated)

  // `engine` here loads all the jobs defined in the "jobs" namespaces
  val engine = system.actorOf(akka.actor.Props(classOf[Engine], Some(true), "jobs"::"jobs2"::"jobs3"::Nil, "workflows" :: Nil))

  "When submitting a ReST call to the Engine to create a workflow" should {
    import WorkflowDummyData._

    "return a HTTP-40x code when json payload is not detected." in {
      val data = notJson.sample.get
      Post(s"/flows/create", HttpEntity(`application/json`, data)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors": ["Expecting a JSON payload in the request, none seen."]}""").getOrElse(Json.Null)
      }
    }

    "return a HTTP-420 code when json payload is detected but not in a recognized format." in {
      val data = jsonButNotWorkflowConfig.sample.get
      Post(s"/flows/create", HttpEntity(`application/json`, data)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors": ["Expecting a JSON payload in the request, none seen."]}""").getOrElse(Json.Null)
      }
    }

    "return a HTTP-420 code when json payload is detected (workflow config is valid format) but Engine rejected it." in {
      import io.circe.generic.auto._, io.circe.syntax._
      val data = validWorkflowConfig.sample.get
      Post(s"/flows/create", HttpEntity(`application/json`, data.asJson.noSpaces)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual UnprocessableEntity
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors": ["JSON payload detected but invalid format."]}""").getOrElse(Json.Null)
      }
    }

    "return a HTTP-420 code when json payload is detected (workflow config is valid format) but Engine rejected it because there's a loop" in {
      import io.circe.generic.auto._, io.circe.syntax._
      val wfConfig =
        WorkflowConfig(id = 42,
                       name = "Test workflow name",
                       description = "Yeah, its a test",
                       jobgraph = List("0 -> 1", "1 -> 1")) // note: jobgraph contains a loop

      Post(s"/flows/create", HttpEntity(`application/json`, wfConfig.asJson.noSpaces)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual UnprocessableEntity
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors": ["JSON payload detected but invalid format."]}""").getOrElse(Json.Null)
      }
    }

    "return a HTTP-420 code when json payload is detected (workflow config is valid format) but Engine rejected it because there's no valid jobgraph" in {
      import io.circe.generic.auto._, io.circe.syntax._
      val wfConfig =
        WorkflowConfig(id = 42,
                       name = "Test workflow name",
                       description = "Yeah, its a test",
                       jobgraph = List("99 -> 98", "98 -> 99")) // note: jobgraph contains references to non-existent job descriptors

      Post(s"/flows/create", HttpEntity(`application/json`, wfConfig.asJson.noSpaces)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual UnprocessableEntity
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors": ["JSON payload detected but invalid format."]}""").getOrElse(Json.Null)
      }
    }

    "return a HTTP-420 code when json payload is detected (workflow config is valid format) but Engine rejected it because there's an already existing workflow in the system" in {
      import io.circe.generic.auto._, io.circe.syntax._
      val wfConfig =
        WorkflowConfig(id = 1,
                       name = "Test workflow name",
                       description = "Yeah, its a test",
                       jobgraph = List("99 -> 98", "98 -> 99")) // note: jobgraph contains references to non-existent job descriptors

      Post(s"/flows/create", HttpEntity(`application/json`, wfConfig.asJson.noSpaces)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual UnprocessableEntity
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors": ["JSON payload detected but invalid format."]}""").getOrElse(Json.Null)
      }
    }

    "return a HTTP-200 code when json payload is detected (workflow config is valid format) and Engine accepted it." in {
      import io.circe.generic.auto._, io.circe.syntax._
      val wfConfig =
        WorkflowConfig(id = 211,
                       name = "Test workflow name",
                       description = "Yeah, its a test",
                       jobgraph = List("0 -> 1", "1 -> 2")) // note: jobgraph is a DAG

      persistence.DbFunctions.clearWfTemplateWhere(211, persistence.Transactors.xa)

      Post(s"/flows/create", HttpEntity(`application/json`, wfConfig.asJson.noSpaces)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"workflow_index": ${wfConfig.id}}""").getOrElse(Json.Null)
      }
    }

  }

  "When starting a workflow (created via ReST submission)" in {

    "return a HTTP-200 code when the workflow submission succeeded when started." in {
      import io.circe.generic.auto._, io.circe.syntax._
      val wfConfig =
        WorkflowConfig(id = 212,
                       name = "Test workflow name",
                       description = "Yeah, its another test",
                       jobgraph = List("0 -> 1", "1 -> 2")) // note: jobgraph is a DAG

      persistence.DbFunctions.clearWfTemplateWhere(212, persistence.Transactors.xa)

      Post(s"/flows/create", HttpEntity(`application/json`, wfConfig.asJson.noSpaces)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"workflow_index": ${wfConfig.id}}""").getOrElse(Json.Null)
      }

      Put(s"/flows/${wfConfig.id}/start") ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        verifyWorkflowIdEmbedded.run(responseAs[io.circe.Json])._1
      }
    }
  }

  "When querying a listing of all workflow(s) in the system" in {

    "return a HTTP-200 code when the workflows exists" in {
      Get("/flows/") ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        verifyWorkflowsEmbedded.run(responseAs[io.circe.Json])
      }
    }

  }

  "When querying for workflow(s) in the system" in {

    "return a HTTP-200 code when the workflows exists" in {

      import io.circe.generic.auto._, io.circe.syntax._
      val wfConfig =
        WorkflowConfig(id = 213,
                       name = "Test workflow name",
                       description = "Yeah, its a test",
                       jobgraph = List("0 -> 1", "1 -> 2")) // note: jobgraph is a DAG

      var workflowId : hicoden.jobgraph.WorkflowId = null

      persistence.DbFunctions.clearWfTemplateWhere(213, persistence.Transactors.xa)

      Post(s"/flows/create", HttpEntity(`application/json`, wfConfig.asJson.noSpaces)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"workflow_index": ${wfConfig.id}}""").getOrElse(Json.Null)
      }

      Put(s"/flows/${wfConfig.id}/start") ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        val j = responseAs[io.circe.Json]
        workflowId = java.util.UUID.fromString(verifyWorkflowIdEmbedded.run(j)._2)
      }

      Get(s"/flows/$workflowId") ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        verifyWorkflowReportEmbedded.run(responseAs[io.circe.Json]).get
      }

    }

  }

  "When starting a workflow" in {

    "the Engine will not validate the job overrides if the payload does not conform to the expected format" in {
      val payload = s"""{}"""
      val fakeWorkflowId = 42
      Put(s"/flows/$fakeWorkflowId/start", HttpEntity(`application/json`, payload)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors":["Requested job overrides failed upon validation"]}""").getOrElse(Json.Null)
      }
    }

    "the Engine will refuse to start a workflow (when the referred index is invalid), when the overrides payload is valid JSON format but the options described is invalid" in {
      val payload = s"""{"overrides":[1]}"""
      val fakeWorkflowId = 0
      Put(s"/flows/$fakeWorkflowId/start", HttpEntity(`application/json`, payload)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors":["Requested job overrides failed upon validation"]}""").getOrElse(Json.Null)
      }
    }

    "the Engine will start a workflow (when the referred index is valid), when the overrides payload is valid JSON format but no options are described" in {
      val payload = s"""{"overrides":[]}"""
      val fakeWorkflowId = 0
      import io.circe.optics.JsonPath._
      val checker = root.workflow_id.string
      Put(s"/flows/$fakeWorkflowId/start", HttpEntity(`application/json`, payload)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        val j = responseAs[io.circe.Json]
        checker.getOption(j) must beSome // the runtime workflow id (which is a UUID-like string) is returned
      }
    }

    "the Engine will start a workflow (referred index exists) when the overrides payload is absent." in {
      val workflowId = 0
      Put(s"/flows/$workflowId/start") ~> WorkflowWebServicesRoutes ~> check {
        import io.circe.optics.JsonPath._
        val checker = root.workflow_id.string
        status shouldEqual OK
        val j = responseAs[io.circe.Json]
        checker.getOption(j) must beSome // the runtime workflow id (which is a UUID-like string) is returned
      }
    }

    "the Engine will refuse to start a workflow (referred index ∉ existing workflow indices) even if the job overrides is (a) valid JSON; (b) job id exists; (c) overrides check out OK" in {
      val payload = s"""{"overrides":[{"id":0, "description":"A test description"}]}""" // simulates the payload from an http client
      val fakeWorkflowId = 9
      Put(s"/flows/$fakeWorkflowId/start", HttpEntity(`application/json`, payload)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors":["No such workflow index/id found: $fakeWorkflowId"]}""").getOrElse(Json.Null)
      }
    }

    "the Engine will refuse to start a workflow (referred index ∉ existing workflow indices) even if the job overrides is (a) valid JSON; (b) job id exists; (c) overrides check out OK" in {
      val payload = s"""{"overrides":[{"id":100, "description":"A test description"}]}""" // simulates the payload from an http client
      val fakeWorkflowId = 99
      Put(s"/flows/$fakeWorkflowId/start", HttpEntity(`application/json`, payload)) ~> WorkflowWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
        responseAs[io.circe.Json] shouldEqual parse(s"""{"errors":["No such workflow index/id found: $fakeWorkflowId"]}""").getOrElse(Json.Null)
      }
    }

  }

}



