package hicoden.jobgraph.fsm.runners

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, _}
import Gen.{containerOfN, choose, pick, mapOf, listOf, listOfN, oneOf}
import Prop.{forAll, throws, AnyOperators}

class DataflowMonitorRunnerSpecs extends Specification with ScalaCheck {
  override def is = sequential ^ s2"""
  Should be able to activate the DataflowRunner and validate the return as 'String' $verifyCanRunDataflowRunnerAndReturnAsString
  Should be able to activate the DataflowRunner and validate the return as 'JSON' $verifyCanRunDataflowRunnerAndReturnAsJson
  Should be able to activate the DataflowRunner and validate the return as 'JSON' $verifyCanRunDataflowRunnerAndReturnAsJson2
  """

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  def verifyCanRunDataflowRunnerAndReturnAsString = {
    import hicoden.jobgraph.fsm._, runners._
    val fakeJobId = "TEST12334341241351241241243213213123213123"
    val ctx =
      MonitorContext(getClass.getClassLoader.getResource("gcloud_monitor_job4.sh").getPath.toString :: Nil, fakeJobId, "")
    val runner = new DataflowMonitorRunner
    val result = runner.run(ctx)((x: String) ⇒ x.trim)
    result.returns must beEqualTo(fakeJobId)
  }

  def verifyCanRunDataflowRunnerAndReturnAsJson = {
    import hicoden.jobgraph.fsm._, runners._
    val fakeJobId = "TEST12334341241351241241243213213123213123"
    val ctx =
      MonitorContext(getClass.getClassLoader.getResource("gcloud_monitor_job2.sh").getPath.toString :: Nil, fakeJobId, _root_.io.circe.Json.Null)
    val runner = new DataflowMonitorRunner
    val result = runner.run(ctx)(jsonParser.parse)
    result.returns must not be_==(_root_.io.circe.Json.Null)
  }

  def verifyCanRunDataflowRunnerAndReturnAsJson2 = {
    import hicoden.jobgraph.fsm._, runners._
    val fakeJobId = "TEST12334341241351241241243213213123213123"
    val ctx =
      MonitorContext(getClass.getClassLoader.getResource("gcloud_monitor_job3.sh").getPath.toString :: Nil, fakeJobId, _root_.io.circe.Json.Null)
    val runner = new DataflowMonitorRunner
    val result = runner.run(ctx)(jsonParser.parse)
    result.returns must be_==(_root_.io.circe.Json.Null)
  }

}
