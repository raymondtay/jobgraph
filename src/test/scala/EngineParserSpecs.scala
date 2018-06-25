package hicoden.jobgraph.configuration.engine

import hicoden.jobgraph.configuration.engine.model._

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, _}
import Gen._
import Prop.{forAll, throws, AnyOperators}

object EngineParserData {
  val invalidNs : List[String] = List("mesos2", "mesos3")
  val validNs : List[String] = List("mesos")

  def genValidNs : Gen[String] = oneOf(validNs)
  def genInvalidNs : Gen[String] = oneOf(invalidNs)

  implicit val arbInvalidNamespaces = Arbitrary(genInvalidNs)
  implicit val arbValidNamespaces = Arbitrary(genValidNs)
}

class EngineParserSpecs extends mutable.Specification with ScalaCheck with Parser {

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  {
    import EngineParserData.arbInvalidNamespaces
    "Invalid namespace keys to load the Engine(s) configuration will result in 'LoadFailure' failure." >> prop { (ns: String) ⇒
      loadDefault(ns).toEither must beLeft((nel: NonEmptyList[HOCONValidation]) ⇒ nel.head.errorMessage must be_==(s"Unable to load configuration from namespace: $ns"))
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineParserData.arbValidNamespaces
    "Valid namespace keys to load the Engine(s) configuration will result in success." >> prop { (ns: String) ⇒
      loadDefault(ns).toEither must beRight((cfg: MesosConfig) ⇒ {
        cfg.enabled must be_==(true)
        cfg.runas must be_==("hicoden")
      })
      loadDefault(ns).toList must not be empty
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

}
