package hicoden.jobgraph.fsm.runners.runtime

import hicoden.jobgraph.fsm.runners.{MonitorContext}
import hicoden.jobgraph.{Job,JobId,WorkflowId}
import hicoden.jobgraph.configuration.step.model._
import hicoden.jobgraph.configuration.workflow.model._

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, _}
import Gen.{containerOfN, frequency,choose, pick, mapOf, listOf, listOfN, oneOf, numStr}
import Prop.{forAll, throws, AnyOperators}

object GoogleDataflowData {
  val emptyGoogleMockJson = "{}"
  val invalidGoogleMockJson = """{"test":42}"""

  val googleMockJson =
  """
  {
    "createTime": "2018-05-31T06:24:09.605580Z",
    "currentState": "JOB_STATE_DONE",
    "currentStateTime": "2018-05-31T06:30:56.227404Z",
    "environment": {
      "userAgent": {
        "name": "Apache Beam SDK for Python",
        "support": {
          "status": "SUPPORTED",
          "url": "https://github.com/apache/beam/releases"
        },
        "version": "2.4.0"
      },
      "version": {
        "job_type": "PYTHON_BATCH",
        "major": "7"
      }
    },
    "id": "2018-05-30_23_24_08-13216803175800099823"
  }
  """ ::
  """
  {
    "createTime": "2018-05-31T06:24:09.605580Z",
    "currentState": "JOB_STATE_RUNNING",
    "currentStateTime": "2018-05-31T06:30:56.227404Z",
    "environment": {
      "userAgent": {
        "name": "Apache Beam SDK for Python",
        "support": {
          "status": "SUPPORTED",
          "url": "https://github.com/apache/beam/releases"
        },
        "version": "2.4.0"
      },
      "version": {
        "job_type": "PYTHON_BATCH",
        "major": "7"
      }
    },
    "id": "2018-05-30_23_24_08-13216803175800099823"
  }
  """ ::
  """
  {
    "createTime": "2018-05-31T06:24:09.605580Z",
    "currentState": "JOB_STATE_FAILED",
    "currentStateTime": "2018-05-31T06:30:56.227404Z",
    "id": "2018-05-30_23_24_08-13216803175800099823"
  }
  """ :: Nil

  val genInvalidGoogleJson = for {
    s ← oneOf(emptyGoogleMockJson, invalidGoogleMockJson)
  } yield s

  val genValidGoogleJson = for {
    s ← oneOf(googleMockJson)
  } yield s

  implicit val arbValidGoogleMockJson   = Arbitrary(genValidGoogleJson)
  implicit val arbInvalidGoogleMockJson = Arbitrary(genInvalidGoogleJson)
}

class GoogleDataflowFunctionSpecs extends mutable.Specification with
                               ScalaCheck with
                               GoogleDataflowJobResultFunctions {

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._
  import _root_.io.circe._, parser._

  {
    import GoogleDataflowData.{arbValidGoogleMockJson}
    "Interpreting Google JSON response. Capture all data points from Google Cloud SDK response JSON." >> prop { (json: String) ⇒
      val dummyJobId = "some dummy job google dataflow job id"
      val ctx = MonitorContext("dummy-program.sh"::Nil, dummyJobId, parse(json).getOrElse(Json.Null))
      interpretJobResult(ctx) must beSome
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GoogleDataflowData.{arbInvalidGoogleMockJson}
    "Interpreting Google JSON response. Unable to capture all data points from Google Cloud SDK response JSON when its invalid." >> prop { (json: String) ⇒
      val dummyJobId = "some dummy job google dataflow job id"
      val ctx = MonitorContext("dummy-program.sh"::Nil, dummyJobId, parse(json).getOrElse(Json.Null))
      interpretJobResult(ctx) must beNone
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    "Interpreting Google JSON response. Unable to capture all data points from Google Cloud SDK response JSON when its invalid e.g. not a JSON but some random data." >> prop { (arbData: String) ⇒
      val dummyJobId = "some dummy job google dataflow job id"
      val ctx = MonitorContext("dummy-program.sh"::Nil, dummyJobId, parse(arbData).getOrElse(Json.Null))
      interpretJobResult(ctx) must beNone
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

}
