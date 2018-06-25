package hicoden.jobgraph.fsm

import scala.concurrent._
import hicoden.jobgraph.engine.GoogleDataflowId
import hicoden.jobgraph.configuration.engine.model.MesosConfig
import hicoden.jobgraph.fsm.runners.runtime.{JobContext, MesosJobContext}

//
// The FSM will use "runners" which interacts with the targeted runners of the
// Apache Beam Framework (e.g. Google Dataflow, Apache Flink, Apache Gearpump,
// Apache Apex, Apache Spark).
//
// So far, we are supporting [[DataflowRunner]]
//
package object runners {

  import hicoden.jobgraph.configuration.step.model.JobConfig

  /**
    * The context carries the payload [[locationOfProgram]] and the returned
    * object/result is carried in the [[returns]] of the type parameter 'A'.
    */
  case class MonitorContext[A](locationOfProgram : List[String], jobId : String, returns: A)

  case class DataflowTerminationContext(locationOfProgram : List[String], jobIds : List[GoogleDataflowId], returns: List[String] = Nil)

  /**
   * The context carries the job's configuration and its only transformed in
   * the [[ExecRunner]] at runtime to a [[JobContext]] which is executed by the
   * concrete runner see [[DataflowRunner]].
   */
  case class ExecContext(jobConfig: JobConfig)

  case class MesosExecContext(jobConfig : JobConfig, mesosCfg : MesosConfig)

  //
  // When a concrete class implements this, the concrete class must implement
  // the [[run]] method which would take the context [[ctx]] and run that
  // against the function [[f]] which would return a fresh context
  //
  trait MonitorRunner {

    /**
      * When a concrete class implements this, it would take the context
      * [[ctx]] and run that against the function [[f]] which would return a
      * fresh context
      * @param ctx the incoming context
      * @param f the function to process the result
      * @return the ctx where the result is injected
      */
    def run[A](ctx: MonitorContext[A])(f: String ⇒ A) : MonitorContext[A]
  }

  //
  // When a concrete class implements this, the concrete class must implement
  // the [[run]] method which would take the context [[ctx]] and run that
  // against the function [[f]] which would return a fresh context
  //
  trait TerminationRunner {

    /**
      * When a concrete class implements this, it would take the context
      * [[ctx]] and run that against the function [[f]] which would return a
      * fresh context
      * @param ctx the incoming context
      * @param f the function to process the result
      * @return the ctx where the result is injected
      */
    def run(ctx: DataflowTerminationContext)(f: String ⇒ String) : DataflowTerminationContext
  }

  trait ExecRunner {

    /**
     * Takes the execution context and executes
     * @param ctx
     * @param f a transformer that converts the configuration to a runtime
     * representation
     * @return A future which carries the [[JobContext]] as payload
     */
    def run(ctx: ExecContext)(f: JobConfig ⇒ runtime.JobContext)(implicit ec: ExecutionContext) : Future[JobContext]
  }

  trait MesosExecRunner {

    /**
     * Takes the execution context in Mesos and executes against the Apache
     * Mesos cluster; A future is returned which carries the result of the
     * execution.
     * @param ctx
     * @param f a transformer that converts the configuration to a runtime
     * representation
     * @return A future which carries the [[JobContext]] as payload
     */
    def run(ctx: MesosExecContext)(f: JobConfig ⇒ runtime.MesosJobContext)(implicit ec: ExecutionContext) : Future[MesosJobContext]
  }

}
