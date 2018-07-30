package hicoden.jobgraph.engine

import pureconfig._
import pureconfig.error._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary.{arbString ⇒ _, arbInt ⇒ _ , _}
import Gen.{containerOfN, frequency,choose, pick, mapOf, listOf, listOfN, oneOf, numStr}
import Prop.{forAll, throws, AnyOperators}

object EngineCliOptsData {

  private def genInvalidOptKey = for {
    k ← oneOf("init", "ini", "i")
  } yield k

  private def genInvalidOptValue = for {
    k ← oneOf("True", "False")
  } yield k

  private def genValidOptValue = for {
    k ← oneOf("yes", "no")
  } yield k

  def genInvalidCliValues = for {
    k ← genInvalidOptKey
    v ← genInvalidOptValue
  } yield s"--$k=$v"

  def genValidCliValues = for {
    v ← genValidOptValue
  } yield s"--initDb=$v"

  implicit val arbValidCliValues = Arbitrary(genValidCliValues)
  implicit val arbInvalidCliValues = Arbitrary(genInvalidCliValues)
}

class EngineCliOptsSpecs extends mutable.Specification with ScalaCheck {
  val minimumNumberOfTests = 20
  import cats._, data._, implicits._, Validated._

  {
    import EngineCliOptsData.arbValidCliValues
    "When cli parser receives an valid key and values, system parsing would succeed." >> prop { (cliValue: String) ⇒
      EngineCliOptsParser.parseCommandlineArgs(cliValue :: Nil) must beSome
    }
  }
  {
    import EngineCliOptsData.arbInvalidCliValues
    "When cli parser receives an invalid key, system would fail" >> prop { (cliValue: String) ⇒
      EngineCliOptsParser.parseCommandlineArgs(cliValue :: Nil) must beNone
    }
  }
  {
    import EngineCliOptsData.arbInvalidCliValues
    "When cli parser receives no command line arguments, system would succeed" >> prop { (cliValue: String) ⇒
      EngineCliOptsParser.parseCommandlineArgs(Nil) must beSome
    }
  }
}
