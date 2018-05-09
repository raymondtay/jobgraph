package hicoden.jobgraph.configuration.step

import hicoden.jobgraph.configuration.step.model._

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, _}
import Gen.{containerOfN, choose, pick, mapOf, listOf, listOfN, oneOf}
import Prop.{forAll, throws, AnyOperators}

object StepLoaderData {
  val validNs : List[String] = List("jobs", "jobs2", "jobs3")
  val invalidNs : List[String] = List("jobX", "jobsY", "jobsZ")

  def genValidNs : Gen[String] = oneOf(validNs)
  def genInvalidNs : Gen[String] = oneOf(invalidNs)

  implicit val arbInvalidNamespaces = Arbitrary(genInvalidNs)
  implicit val arbValidNamespaces = Arbitrary(genValidNs)
}

class StepLoaderSpecs extends mutable.Specification with ScalaCheck with Parser with Loader {

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  {
    import StepLoaderData.arbInvalidNamespaces
    "Using invalid namespace keys to lookup the step(s) will result in failures." >> prop { (ns: String) ⇒
      val loadedConfigs = loadDefault(ns :: Nil)
      loadDefault(ns::Nil).toEither must beLeft((nel: NonEmptyList[HOCONValidation]) ⇒ nel.head.errorMessage(ns) must be_==(s"Unable to load configuration from namespace: $ns"))
      val jdt : JobDescriptorTable = scala.collection.immutable.HashMap.empty[Int, JobConfig]
      hydrateJobConfigs(loadedConfigs.toList.flatten).runS(jdt).value.size must be_==(0)
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import StepLoaderData.arbValidNamespaces
    "Using valid namespace keys to load the step(s) configuration will result in success." >> prop { (ns: String) ⇒
      val loadedConfigs = loadDefault(ns :: Nil)
      loadedConfigs.toEither must beRight((cfgs: List[JobConfig]) ⇒ cfgs.size must beBetween(1,2))
      loadedConfigs.toList must not be empty
      val jdt : JobDescriptorTable = scala.collection.immutable.HashMap.empty[Int, JobConfig]
      hydrateJobConfigs(loadedConfigs.toList.flatten).runS(jdt).value.size must beBetween(1, 2)
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import StepLoaderData.arbValidNamespaces
    "Lookuping the JDT with valid jobConfigs will result in success" >> prop { (ns: String) ⇒
      val loadedConfigs = loadDefault(ns :: Nil)
      loadedConfigs.toEither must beRight((cfgs: List[JobConfig]) ⇒ cfgs.size must beBetween(1,2))
      loadedConfigs.toList must not be empty
      var jdt : JobDescriptorTable = scala.collection.immutable.HashMap.empty[Int, JobConfig]
      jdt = hydrateJobConfigs(loadedConfigs.toList.flatten).runS(jdt).value
      jdt.contains(loadedConfigs.toList.flatten.head.id) must beTrue
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

}
