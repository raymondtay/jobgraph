package hicoden.jobgraph.fsm.runners


import hicoden.jobgraph.configuration.step.model.JobConfig
import scala.sys.process._
import com.typesafe.scalalogging.Logger

// 
// JobGraph Dataflow Runner. See [[DataflowRunnerSpecs]] for the unit tests
// or check out the sample code via `sbt run` on the sbt repl.
//
class DataflowRunner extends ExecRunner {

  import cats._, data._, implicits._

  val logger = Logger(classOf[DataflowRunner])

  //
  // The runtime representation of the context would be created from the job's
  // configuration via [[f]] and once its done, jobgraph would launch the
  // process and captures whatever the output it produces. For now, jobgraph is
  // not expecting any kind of return back to the service - this might change
  // in the future.
  //
  def run(ctx: ExecContext)(f: JobConfig ⇒runtime.JobContext) : Unit = {
    val _ctx = f(ctx.jobConfig)
    val result : Either[Throwable,String] = scala.util.Try{
      "".!!
    }.toEither
    result.fold(onError(_ctx), onSuccess(_ctx))  
  }
 
  // If error occurs, a log is produced and we do not alter the context but
  // return it
  def onError(ctx: runtime.JobContext) = (error: Throwable) ⇒ {
    logger.error(s"Unable to trigger program with this error: $error")
    ctx
  }

  def onSuccess(ctx: runtime.JobContext) = (data: String) ⇒ {
    logger.info("About to parse the return data and validate it.")
    ctx
  }

}

class DataflowMonitorRunner extends MonitorRunner {

  import cats._, data._, implicits._

  val logger = Logger(classOf[DataflowMonitorRunner])

  //
  //The program given at the [[locationOfProgram]] would execute with the 
  //environment variable of JOB_ID (that is google's requirement) and we
  //capture what we see (along with the errors)
  //
  def run[A](ctx: MonitorContext[A])(f: String ⇒ A) : MonitorContext[A] = {
    val result : Either[Throwable,String] = scala.util.Try{
      Process(ctx.locationOfProgram, cwd = None, extraEnv = "JOB_ID" -> ctx.jobId).!!
    }.toEither
    result.fold(onError(ctx), onSuccess(ctx)(f))  
  }
 
  // If error occurs, a log is produced and we do not alter the context but
  // return it
  def onError[A](ctx: MonitorContext[A]) = (error: Throwable) ⇒ {
    logger.error(s"Unable to trigger program with this error: $error")
    ctx
  }

  // The data string should be a JSON string (afaik from Google's GCloud sdk)
  // and we shall attempt to parse it and return the JSON object for further
  // processing.
  def onSuccess[A](ctx: MonitorContext[A])(f: String ⇒ A) = (data: String) ⇒ {
    logger.info("About to parse the return data and validate it.")
    ctx.copy(returns = f(data))
  }

}

object jsonParser {
  import io.circe._, io.circe.parser.{parse ⇒ cparse, _}

  def parse(data: String) = cparse(data).getOrElse(Json.Null)
}


