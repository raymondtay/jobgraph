package hicoden.jobgraph.fsm.runners

//
// The [[runtime]] is the placeholder for the functionality that occurs at
// runtime i.e. from the perspective of supporting all the functionality
// related to running the jobs
// @author Raymond Tay
// @version 1.0
//
package object runtime {

  import hicoden.jobgraph.cc.{LRU, StatsMiner}
  import cats._, data._, implicits._
  import hicoden.jobgraph.{JobId, WorkflowId}
  import hicoden.jobgraph.cc.{HttpService, SchedulingAlgorithm, LRU}
  import hicoden.jobgraph.configuration.engine.model.{MesosConfig, JobgraphConfig}
  import hicoden.jobgraph.configuration.step.model.{Runner, JobConfig}
  import akka.actor.ActorSystem
  import akka.stream.ActorMaterializer
  import scala.concurrent._
  import scala.concurrent.duration._

  // Represents the location of this "JobEngine"
  case class JobEngineLocationContext(hostname : String, port: Int)

  // This is pretty much the runtime representation of the job's configuration
  // in [[JobConfig]].
  case class JobContext(name: String, description: String, workdir: String, timeout : Int, workflowId: WorkflowId, jobId: JobId, location: JobEngineLocationContext, runner: Runner)

  // This represents the contextual information needed for the job. Take note
  // that this abstraction gives us the ability to deploy jobs to different
  // Apache Mesos running in the data centers.
  //
  // @param taskExec /path/to/executable-mesos-scio-job
  // @param runAs the "user" that Apache Mesos will register this job with
  // @param jobCtx jobContext
  // @param mesosCtx contextual info regarding the Apache Mesos cluster
  case class MesosJobContext(taskExec : String, runAs: String, jobCtx: JobContext, mesosCtx: MesosRuntimeConfig)

  case class MesosRuntimeConfig(enabled: Boolean, runas: String, hostname: String, hostport: Int)

  // [[JobContextManifest]] is the typeclass where all functions related to
  // transforming [[JobConfig]] to [[JobContext]]es so that the runner can use
  // during the execution run of the Job s.t. [[JobConfig]] remains immutable
  //
  // If mesos is enabled we arranged for the LRU algorithm to be applied using
  // the statistics we have gathered at the time of dispatch.
  //
  trait JobContextManifest extends StatsMiner {
    def manifest(wfId: WorkflowId, jobId: JobId, jgCfg: JobgraphConfig) =
      (cfg: JobConfig)⇒
        JobContext(name = cfg.name, description = cfg.description,
                   workdir = cfg.workdir, timeout = cfg.timeout,
                   workflowId = wfId,
                   jobId = jobId, location = JobEngineLocationContext(jgCfg.hostname, jgCfg.hostport),
                   runner = cfg.runner)

    def manifestMesos(wfId: WorkflowId,
                      jobId: JobId,
                      mesosCfg: MesosConfig,
                      jgCfg: JobgraphConfig,
                      httpService : HttpService,
                      selector: (SchedulingAlgorithm, HttpService) => Reader[MesosConfig, MesosRuntimeConfig])(implicit scheduler: SchedulingAlgorithm) =
      (cfg: JobConfig)⇒ {
        MesosJobContext(
          taskExec = "MesosScioJob",
          runAs = mesosCfg.runas,
          JobContext(name = cfg.name, description = cfg.description,
                   workdir = cfg.workdir, timeout = cfg.timeout,
                   workflowId = wfId,
                   jobId = jobId, location = JobEngineLocationContext(jgCfg.hostname, jgCfg.hostport),
                   runner = cfg.runner), selector(scheduler, httpService)(mesosCfg))
      }

  }

}
