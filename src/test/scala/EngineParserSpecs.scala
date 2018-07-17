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
  val invalidMesosNs : List[String] = List("mesos2", "mesos3")
  val validMesosNs : List[String] = List("mesos")
  val invalidEngineNs : List[String] = List("jobgraph2", "jobgraph3")
  val validEngineNs : List[String] = List("jobgraph")

  def genValidMesosNs : Gen[String] = oneOf(validMesosNs)
  def genInvalidMesosNs : Gen[String] = oneOf(invalidMesosNs)
  def genValidEngineNs : Gen[String] = oneOf(validEngineNs)
  def genInvalidEngineNs : Gen[String] = oneOf(invalidEngineNs)

  implicit val arbInvalidMesosNamespaces = Arbitrary(genInvalidMesosNs)
  implicit val arbValidMesosNamespaces = Arbitrary(genValidMesosNs)
  implicit val arbInvalidEngineNamespaces = Arbitrary(genInvalidEngineNs)
  implicit val arbValidEngineNamespaces = Arbitrary(genValidEngineNs)
}

class EngineParserSpecs extends mutable.Specification with ScalaCheck with Parser {

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  {
    import EngineParserData.arbInvalidMesosNamespaces
    "Invalid namespace keys to load the Compute Cluster(s) configuration will result in 'LoadFailure' failure." >> prop { (ns: String) ⇒
      loadMesosDefaults(ns).toEither must beLeft((nel: NonEmptyList[HOCONValidation]) ⇒ nel.head.errorMessage must be_==(s"Unable to load configuration from namespace: $ns"))
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineParserData.arbValidMesosNamespaces
    "Valid namespace keys to load the Compute Cluster(s) configuration will result in success." >> prop { (ns: String) ⇒
      loadMesosDefaults(ns).toEither must beRight((cfg: MesosConfig) ⇒ {
        cfg.enabled must be_==(false)
        cfg.runas must be_==("hicoden")
      })
      loadMesosDefaults(ns).toList must not be empty
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineParserData.arbInvalidEngineNamespaces
    "Invalid namespace keys to load the Engine(s) configuration will result in 'LoadFailure' failure." >> prop { (ns: String) ⇒
      loadEngineDefaults(ns).toEither must beLeft((nel: NonEmptyList[HOCONValidation]) ⇒ nel.head.errorMessage must be_==(s"Unable to load configuration from namespace: $ns"))
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineParserData.arbValidEngineNamespaces
    "Valid namespace keys to load the Engine(s) configuration will result in success." >> prop { (ns: String) ⇒
      loadEngineDefaults(ns).toEither must beRight((cfg: JobgraphConfig) ⇒ {
        cfg.hostname must be_==("localhost")
        cfg.hostport must be_==(9000)
      })
      loadEngineDefaults(ns).toList must not be empty
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

}
