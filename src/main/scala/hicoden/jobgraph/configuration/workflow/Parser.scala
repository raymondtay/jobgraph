package hicoden.jobgraph.configuration.workflow

import scala.util.Try
import hicoden.jobgraph.configuration.workflow.model._

trait HOCONWorkflowValidation {
  def errorMessage(namespace: String) = s"Unable to load workflow configuration from namespace: $namespace"
}

object LoadWorkflowFailure extends HOCONWorkflowValidation

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
  type LoadingResult[A] = ValidatedNel[HOCONWorkflowValidation, A]

  /**
    * Attempts to loads the configuration file (by default, it loads
    * "application.conf") with the given set of namespaces.
    *
    * Notes:
    * (a) Make sure you use the HOCON "includes" to make your configuration known.
    * (b) Make sure you use unique values for your namespaces
    *
    * @param namespace the HOCON conforming namespace key(s) which should point to the array of configuration for "steps"
    * @return Either.Left(errors) if errors else Either.Right(container of [[WorkflowConfig]])
    */
  def loadDefault : Reader[List[String], LoadingResult[List[WorkflowConfig]]] =
    Reader{ (namespaces: List[String]) ⇒
      namespaces.map(namespace ⇒ loadDefaultByNamespace(namespace)).reduce(_ |+| _)
    }

  /**
    * Attempts to loads the configuration file (by default, it loads
    * "application.conf") with the given namespace.
    *
    * Notes:
    * (a) Make sure you use the HOCON "includes" to make your configuration known.
    * (b) Make sure you use unique value for your namespaces
    *
    * @param namespace the HOCON conforming namespace key which should point to the array of configuration for "steps"
    * @return Either.Left(errors) if errors else Either.Right(container of [[WorkflowConfig]])
    */
  def loadDefaultByNamespace : Reader[String, LoadingResult[List[WorkflowConfig]]] =
    Reader{ (namespace: String) ⇒
      Try{ loadConfigOrThrow[List[WorkflowConfig]](namespace) }.toOption match {
        case Some(cfgs) ⇒ cfgs.validNel
        case None ⇒ LoadWorkflowFailure.invalidNel
      }
    }

}

/*
 * Factory singleton object
 */
object Parser {
  def apply = new Parser{}
}

