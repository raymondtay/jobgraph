package hicoden.jobgraph.fsm.runners

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, _}
import Gen.{containerOfN, choose, pick, mapOf, listOf, listOfN, oneOf}
import Prop.{forAll, throws, AnyOperators}

class DataflowRunnerSpecs extends Specification with ScalaCheck {
  override def is = sequential ^ s2"""
  """

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  def verifyCanRunDataflowRunnerAndReturnAsString = {
    import hicoden.jobgraph.fsm._, runners._
  }
  
}
