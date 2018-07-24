package hicoden.jobgraph.configuration.engine

import scala.util.Try
import hicoden.jobgraph.configuration.engine.model._

trait HOCONValidation {
  val namespace : String
  def errorMessage = s"Unable to load configuration from namespace: $namespace"
}

case object NamespaceNotFound extends HOCONValidation { val namespace : String = "no namespaces provided." }
case class LoadFailure(namespace: String) extends HOCONValidation

/**
  * Read the Human-Optimized Config Object Notation specification
  * [here](https://github.com/lightbend/config/blob/master/HOCON.md)
  *
  * @author Raymond Tay
  * @version 1.0
  */
trait Parser {
  import cats._, data._, implicits._, Validated._
  import pureconfig._
  type LoadingResult[A] = ValidatedNel[HOCONValidation, A]

  /**
    * Attempts to loads the configuration file (by default, it loads
    * "application.conf") with the given set of namespaces.
    *
    * Notes:
    * (a) Make sure you use the HOCON "includes" to make your configuration known.
    * (b) Make sure you use unique values for your namespaces
    *
    * @param namespace the HOCON conforming namespace key(s) which should point to the array of configuration for "steps"
    * @return Either.Left(errors) if errors else Either.Right(container of [[MesosConfig]])
    */
  def loadMesosDefaults: Reader[String, LoadingResult[MesosConfig]] =
    Reader{ (namespace: String) ⇒
      Try{ loadConfigOrThrow[MesosConfig](namespace) }.toOption match {
        case Some(cfg) ⇒ cfg.validNel
        case None ⇒ LoadFailure(namespace).invalidNel
      }
    }

  def loadEngineDefaults: Reader[String, LoadingResult[JobgraphConfig]] =
    Reader{ (namespace: String) ⇒
      Try{ loadConfigOrThrow[JobgraphConfig](namespace) }.toOption match {
        case Some(cfg) ⇒ cfg.validNel
        case None ⇒ LoadFailure(namespace).invalidNel
      }
    }

  def loadEngineDb : Reader[String, LoadingResult[JobgraphDb]] = 
    Reader{ (namespace: String) ⇒
      Try{ loadConfigOrThrow[JobgraphDb](namespace) }.toOption match {
        case Some(cfg) ⇒ cfg.validNel
        case None ⇒ LoadFailure(namespace).invalidNel
      }
    }
}

/*
 * Factory singleton object
 */
object Parser {
  def apply = new Parser{}
}

