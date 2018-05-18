package hicoden.jobgraph.fsm

//
// The FSM will use "runners" which interacts with the targeted runners of the
// Apache Beam Framework (e.g. Google Dataflow, Apache Flink, Apache Gearpump,
// Apache Apex, Apache Spark).
//
// So far, we are supporting [[DataflowRunner]]
//
package object runners {

  /**
    * The context carries the payload [[locationOfProgram]] and the returned
    * object/result is carried in the [[returns]] of the type parameter 'A'.
    */
  case class Context[A](locationOfProgram : List[String], jobId : String, returns: A)

  //
  // When a concrete class implements this, the concrete class must implement
  // the [[run]] method which would take the context [[ctx]] and run that
  // against the function [[f]] which would return a fresh context
  //
  trait Runner {

    /**
      * When a concrete class implements this, it would take the context
      * [[ctx]] and run that against the function [[f]] which would return a
      * fresh context
      * @param ctx the incoming context
      * @param f the function to process the result
      * @return the ctx where the result is injected
      */
    def run[A](ctx: Context[A])(f: String ⇒ A) : Context[A]
  }

}
