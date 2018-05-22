package hicoden.jobgraph.configuration.step

import scala.util.Try
import hicoden.jobgraph.configuration.step.model.{ExecType, RunnerType}
import hicoden.jobgraph.configuration.step.model._

trait HOCONValidation {
  val namespace : String
  def errorMessage = s"Unable to load configuration from namespace: $namespace"
}

case class LoadFailure(namespace: String) extends HOCONValidation
case class RunnerTypeFailure(namespace: String) extends HOCONValidation {
  override
  def errorMessage = s"Allowed namespaces are ${RunnerType.values.mkString} but you have: $namespace"
}

case class ExecTypeFailure(namespace: String) extends HOCONValidation {
  override
  def errorMessage = s"Allowed namespaces are ${ExecType.values.mkString} but you have: $namespace"
}

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
    * @return Either.Left(errors) if errors else Either.Right(container of [[JobConfig]])
    */
  def loadDefault : Reader[List[String], LoadingResult[List[JobConfig]]] =
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
    * @return Either.Left(errors) if errors else Either.Right(container of [[JobConfig]])
    */
  def loadDefaultByNamespace : Reader[String, LoadingResult[List[JobConfig]]] =
    Reader{ (namespace: String) ⇒
      Try{ loadConfigOrThrow[List[JobConfig]](namespace) }.toOption match {
        case Some(cfgs) ⇒
          cfgs.map{cfg ⇒
              val splitted = cfg.runner.runner.split(":")
              if (splitted.size != 2) LoadFailure(namespace).invalidNel[JobConfig]
              else {
                val (left, right) = (splitted(0), splitted(1))
                (validateRunnerType(left), validateExecType(right)).mapN((r, e) ⇒ if (r == None) RunnerTypeFailure(namespace).invalidNel[JobConfig] else if (e == None) ExecTypeFailure(namespace).invalidNel[JobConfig] else cfg.validNel[HOCONValidation])
              }
          }.sequence
        case None ⇒ LoadFailure(namespace).invalidNel
      }
    }

  def validateRunnerType : Reader[String, Option[RunnerType.RunnerType]] = 
    Reader{ (s: String) ⇒
      import RunnerType._
      Try{ RunnerType.withName(s) }.toOption.fold(none[RunnerType])((e: RunnerType) ⇒ e.some)
    }

  def validateExecType : Reader[String, Option[ExecType.ExecType]] = 
    Reader{ (s: String) ⇒
      import ExecType._
      Try{ ExecType.withName(s) }.toOption.fold(none[ExecType])((e: ExecType) ⇒ e.some)
    }

}

/*
 * Factory singleton object
 */
object Parser {
  def apply = new Parser{}
}

