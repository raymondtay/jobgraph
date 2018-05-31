package hicoden.jobgraph.fsm.runners.runtime


object Functions {
  import cats._, data._, implicits._
  import hicoden.jobgraph.configuration.step.model.{JobConfig, ExecType, RunnerType}
  import ExecType._, RunnerType._

  def isPythonModule = Reader{ (cfg: JobContext) ⇒ cfg.runner.runner.split(":")(1) equals ExecType.python.toString }
  def isJavaModule = Reader{ (cfg: JobContext) ⇒ cfg.runner.runner.split(":")(1) equals ExecType.java.toString }

  //
  // This function would throw a [[UnsupportedOperationException]] and its
  // intentional because of the fact that it should not have happened.
  //
  def buildCommand = Reader{ (cfg: JobContext) ⇒
    if (isPythonModule(cfg)) buildPythonCommand(cfg) else
    if (isJavaModule(cfg)) buildJavaCommand(cfg) else throw new UnsupportedOperationException(s"Jobgraph only supports the following exception types : ${ExecType.values.mkString(",")}")
  }

  def buildPythonCommand = Reader{ (cfg: JobContext) ⇒
    (s"python -m ${cfg.runner.module} ${cfg.runner.cliargs.mkString(" ")}", cfg.workdir, Map("PYTHONPATH" -> cfg.workdir))
  }

  def buildJavaCommand = Reader{ (cfg: JobContext) ⇒
    (s"${cfg.runner.module} ${cfg.runner.cliargs.mkString(" ")}", cfg.workdir, Map())
  }

}

//
// Lenses / Optics code for traversing the returned JSON structure for further
// analysis. Make sure that the code here is thread safe. The functions here
// are targeted at the following structure:
//
// {
//  "createTime": "2018-05-31T06:24:09.605580Z",
//  "currentState": "JOB_STATE_DONE",
//  "currentStateTime": "2018-05-31T06:30:56.227404Z",
//  "environment": {
//    "userAgent": {
//      "name": "Apache Beam SDK for Python",
//      "support": {
//        "status": "SUPPORTED",
//        "url": "https://github.com/apache/beam/releases"
//      },
//      "version": "2.4.0"
//    },
//    "version": {
//      "job_type": "PYTHON_BATCH",
//      "major": "7"
//    }
//  },
//  "id": "2018-05-30_23_24_08-13216803175800099823",
//  "location": "us-central1",
//  "name": "beamapp-hicoden-0531062403-523675",
//  "projectId": "hicoden",
//  "stageStates": [
//    {
//      "currentStateTime": "2018-05-31T06:29:13.505Z",
//      "executionStageName": "F18",
//      "executionStageState": "JOB_STATE_DONE"
//    },
//    ...
//    ...
//    ... omitted for brievity sake.
//    {
//      "currentStateTime": "2018-05-31T06:29:05.348Z",
//      "executionStageName": "F19",
//      "executionStageState": "JOB_STATE_DONE"
//    }
//  ],
//  "type": "JOB_TYPE_BATCH"
//}
//
//

//
// Derived from [Google Cloud SDK](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState)
//
object GoogleDataflowJobStatuses extends Enumeration {
  type GoogleDataflowJobStatus = Value
  val JOB_STATE_UNKNOWN, JOB_STATE_STOPPED, JOB_STATE_RUNNING, JOB_STATE_DONE, JOB_STATE_FAILED, JOB_STATE_CANCELLED, JOB_STATE_UPDATED, JOB_STATE_DRAINING, JOB_STATE_DRAINED, JOB_STATE_PENDING, JOB_STATE_CANCELLING = Value
}

trait GoogleDataflowJobResultFunctions {
  import cats._, data._, implicits._
  import io.circe._
  import io.circe.optics.JsonPath._
  import hicoden.jobgraph.fsm.runners.MonitorContext
  import GoogleDataflowJobStatuses._

  def getId : Reader[Json, Option[String]] = Reader{ (j:Json) ⇒ root.id.string.getOption(j) }
  def getCreateTime : Reader[Json, Option[String]] = Reader{ (j:Json) ⇒ root.createTime.string.getOption(j) }
  def getCurrentState : Reader[Json, Option[GoogleDataflowJobStatus]] =
    Reader{ (j:Json) ⇒ root.currentState.string.getOption(j).fold(JOB_STATE_UNKNOWN.some)(e ⇒ scala.util.Try{GoogleDataflowJobStatuses.withName(e)}.toOption) }
  def getCurrentStateTime : Reader[Json, Option[String]] = Reader{ (j:Json) ⇒ root.currentStateTime.string.getOption(j) }

  /**
    * Attempts to lift a couple of values from the returned json carried in the
    * MonitorContext object, that's passed in.
    * @param ctx the context assumes that json is carried in the "returns"
    *            field
    * @return either a None or a 4-tuple which contains the "google dataflow
    *         id", "create time from google", "status", "the date time of the current
    *         state"
    */
  def interpretJobResult : Reader[MonitorContext[Json], Option[(String,String,GoogleDataflowJobStatuses.GoogleDataflowJobStatus,String)]]= Reader{ (ctx: MonitorContext[Json]) ⇒
    ctx.returns == null match {
      case true  ⇒ none
      case false ⇒ 
        (getId(ctx.returns), getCreateTime(ctx.returns), getCurrentState(ctx.returns), getCurrentStateTime(ctx.returns)).mapN(
          (a,b,c,d) ⇒
            (a, b, c, d).mapN(
              (id, createTime, currentState, currentStateTime) ⇒ (id, createTime, currentState, currentStateTime))
            )
    } 
  }
}

object GoogleDataflowFunctions extends GoogleDataflowJobResultFunctions

