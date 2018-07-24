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
  val validEngineDbNs : List[String] = List("jobgraph.db")
  val invalidEngineDbNs : List[String] = List("jobgraph")

  def genValidMesosNs : Gen[String] = oneOf(validMesosNs)
  def genInvalidMesosNs : Gen[String] = oneOf(invalidMesosNs)
  def genValidEngineNs : Gen[String] = oneOf(validEngineNs)
  def genInvalidEngineNs : Gen[String] = oneOf(invalidEngineNs)
  def genValidEngineDbNs : Gen[String] = oneOf(validEngineDbNs)
  def genInvalidEngineDbNs : Gen[String] = oneOf(invalidEngineDbNs)

  implicit val arbInvalidMesosNamespaces = Arbitrary(genInvalidMesosNs)
  implicit val arbValidMesosNamespaces = Arbitrary(genValidMesosNs)
  implicit val arbInvalidEngineNamespaces = Arbitrary(genInvalidEngineNs)
  implicit val arbValidEngineNamespaces = Arbitrary(genValidEngineNs)
  implicit val arbValidEngineDbNamespaces = Arbitrary(genValidEngineDbNs)
  implicit val arbInvalidEngineDbNamespaces = Arbitrary(genInvalidEngineDbNs)
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

  {
    import EngineParserData.arbInvalidEngineDbNamespaces
    "Invalid namespace keys to load the Engine's database will result in failures." >> prop { (ns: String) ⇒
      loadEngineDb(ns).toEither must beLeft
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import EngineParserData.arbValidEngineDbNamespaces
    "Valid namespace keys to load the Engine's database will result in success." >> prop { (ns: String) ⇒
      loadEngineDb(ns).toEither must beRight((cfg: JobgraphDb) ⇒{
        cfg.name must be_==("some_jobgraph_db")
        cfg.username must be_==("admin")
        cfg.password must be_==("admin")
      })
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }
}
