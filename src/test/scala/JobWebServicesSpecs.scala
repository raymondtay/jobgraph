package hicoden.jobgraph.engine

import hicoden.jobgraph.configuration.step.model.{Runner, JobConfig, Restart, RunnerType, ExecType}

import org.specs2.mutable.Specification
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.Specs2RouteTest
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

object JobConfigDummyData {

  private val validRunner : Gen[Runner] = for {
    module ← alphaStr
    runner ← oneOf(RunnerType.Dataflow, RunnerType.MesosDataflow)
    execType ← oneOf(ExecType.python, ExecType.java)
  } yield Runner(module, s"$runner:$execType", Nil)

  private val invalidRunner : Gen[Runner] = for {
    module ← alphaStr
    runner ← oneOf("Flink", "Apex")
    execType ← oneOf("php", "exe")
  } yield Runner(module, s"$runner:$execType", Nil)

  private def genRestartPolicy = for {
    attempts ← choose(1, 5)
  } yield Restart(attempts)

  val validJobConfigs : Gen[JobConfig] = for {
    name ← alphaStr
    description ← alphaStr
    workdir ← alphaStr
    sessionid ← alphaStr
    runner ← validRunner
    restart ← genRestartPolicy
  } yield JobConfig(id = 33, name, description, workdir, sessionid, restart, runner, Nil, Nil)

  val validJobConfigs2 : Gen[JobConfig] = for {
    name ← alphaStr
    description ← alphaStr
    workdir ← alphaStr
    sessionid ← alphaStr
    runner ← validRunner
    restart ← genRestartPolicy
  } yield JobConfig(id = 34, name, description, workdir, sessionid, restart, runner, Nil, Nil)

  val invalidJobConfigs3 : Gen[io.circe.Json] =
    oneOf(
      oneOf(
        io.circe.parser.parse("{}").getOrElse(io.circe.Json.Null),
        io.circe.parser.parse("").getOrElse(io.circe.Json.Null),
        io.circe.parser.parse("""{"id":blah-blah}""").getOrElse(io.circe.Json.Null),
      ),
      oneOf(
        io.circe.parser.parse("{}").getOrElse(io.circe.Json.Null),
        io.circe.parser.parse("""{"id:9999, runner: {"module": "python.test"}"}""").getOrElse(io.circe.Json.Null),
        io.circe.parser.parse("""{"id":blah-blah}""").getOrElse(io.circe.Json.Null),
      ))


  private
  val jobConfigWithExistingId : Gen[JobConfig] = for {
    name ← alphaStr
    description ← alphaStr
    workdir ← alphaStr
    sessionid ← alphaStr
    runner ← validRunner
    restart ← genRestartPolicy
  } yield JobConfig(id = 33, name, description, workdir, sessionid, restart, runner, Nil, Nil)

  private
  val jobConfigWithNonExistingIdInvalidRunner : Gen[JobConfig] = for {
    name ← alphaStr
    description ← alphaStr
    workdir ← alphaStr
    sessionid ← alphaStr
    runner ← invalidRunner
    restart ← genRestartPolicy
  } yield JobConfig(id = 0, name, description, workdir, sessionid, restart, runner, Nil, Nil)

  val invalidJobConfigs = oneOf(jobConfigWithExistingId, jobConfigWithNonExistingIdInvalidRunner)
}

trait JobSpecsFunctions {
  
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
  // looking for; in this case i'm looking for an array w/o inspecting the
  // elements
  private[engine]
  def verifyJobsEmbedded = Reader{ (json: io.circe.Json) ⇒
    import io.circe.optics.JsonPath._
    val f = root.arr
    f.getOption(json) match {
      case None    ⇒ false
      case Some(_) ⇒ true
    }
  }

  // Use lenses to inspect whether the returned ADT contains the key i'm
  // looking for; in this case i'm looking for an array w/o inspecting the
  // elements
  private[engine]
  def verifyJobsEmbeddedNonEmpty = Reader{ (json: io.circe.Json) ⇒
    import io.circe.optics.JsonPath._
    val f = root.arr
    f.getOption(json) match {
      case None    ⇒ false
      case Some(xs) if xs.length == 0 ⇒ true
      case Some(xs) if xs.length != 0 ⇒ false
    }
  }

}

class JobWebServicesSpecs extends Specification with Specs2RouteTest with JobWebServices with JobSpecsFunctions {

  import cats._, data._, implicits._
  import io.circe._, io.circe.parser._

  val actorSystem = system
  val actorMaterializer = materializer

  val engine = system.actorOf(akka.actor.Props(classOf[Engine], "jobs"::"jobs2"::"jobs3"::"jobs4"::Nil, "workflows"::Nil))

  sequential // all specifications are run sequentially

  "When creating a job in the system" in {

    // Job validation, atm, is merely checking whether the id was previously
    // defined and we don't check for the likeness of the contents of the
    // [[JobConfig]]
    "return a HTTP-200 code when the job does not exist in the system and the job is registered." in {
      import JobConfigDummyData._
      import io.circe.syntax._
      import hicoden.jobgraph.engine.runtime.{jobConfigEncoder, runnerConfigEncoder}

      Post(s"/jobs/create", HttpEntity(`application/json`, validJobConfigs.sample.get.asJson.noSpaces)) ~> JobWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
      }
    }

    "return a HTTP-420 code when the job request is not conforming to the JSON format we expect." in {
      import JobConfigDummyData._
      import io.circe.syntax._
      import hicoden.jobgraph.engine.runtime.{jobConfigEncoder, runnerConfigEncoder}

      Post(s"/jobs/create", HttpEntity(`application/json`, invalidJobConfigs3.sample.get.asJson.noSpaces)) ~> JobWebServicesRoutes ~> check {
        status shouldEqual BadRequest
        mediaType shouldEqual `application/json`
      }
    }
 
    "return a HTTP-420 code when the job does exist in the system and the job is not registered." in {
      import JobConfigDummyData._
      import io.circe.syntax._
      import hicoden.jobgraph.engine.runtime.{jobConfigEncoder, runnerConfigEncoder}

      Post(s"/jobs/create", HttpEntity(`application/json`, validJobConfigs2.sample.get.asJson.noSpaces)) ~> JobWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
      }
      Post(s"/jobs/create", HttpEntity(`application/json`, invalidJobConfigs.sample.get.asJson.noSpaces)) ~> JobWebServicesRoutes ~> check {
        status shouldEqual UnprocessableEntity
        mediaType shouldEqual `application/json`
      }
    }
 
  }

  "When querying for job(s) in the system" in {

    "return a HTTP-200 code when the jobs do exist in the system." in {
      Get(s"/jobs/") ~> JobWebServicesRoutes ~> check {
        status shouldEqual OK
        mediaType shouldEqual `application/json`
        verifyJobsEmbedded.run(responseAs[io.circe.Json])
      }
    }

  }

}
