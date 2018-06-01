package hicoden.jobgraph.fsm.runners

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, _}
import Gen.{containerOfN, choose, pick, mapOf, listOf, listOfN, oneOf}
import Prop.{forAll, throws, AnyOperators}

class DataflowJobTerminationRunnerSpecs extends Specification with ScalaCheck {
  override def is = sequential ^ s2"""
  Should be able to activate the DataflowRunner and validate the return as 'String' when job is cancelled successfully. $verifyCanRunDataflowRunnerAndReturnAsString
  Should be able to activate the DataflowRunner and validate the return as 'String' when job is not recognized by Google Dataflow. $verifyCanRunDataflowRunnerAndReturnAsString2
  """

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  def verifyCanRunDataflowRunnerAndReturnAsString = {
    import hicoden.jobgraph.fsm._, runners._
    val fakeJobId = "GOOGLE_ISSUED_JOB_ID_2018"
    val ctx =
      DataflowTerminationContext(getClass.getClassLoader.getResource("gcloud_cancel_job.sh").getPath.toString :: Nil, fakeJobId :: Nil)
    val runner = new DataflowJobTerminationRunner
    val result = runner.run(ctx)((x: String) ⇒ x.trim)
    result.returns.head must startWith(s"Cancelled job [$fakeJobId]")
  }

  def verifyCanRunDataflowRunnerAndReturnAsString2 = {
    import hicoden.jobgraph.fsm._, runners._
    val fakeJobId = "GOOGLE_ISSUED_JOB_ID_2018"
    val ctx =
      DataflowTerminationContext(getClass.getClassLoader.getResource("gcloud_cancel_job2.sh").getPath.toString :: Nil, fakeJobId :: Nil)
    val runner = new DataflowJobTerminationRunner
    val result = runner.run(ctx)((x: String) ⇒ x.trim)
    result.returns.head must startWith(s"Failed to cancel job [$fakeJobId]")
  }
  
}
