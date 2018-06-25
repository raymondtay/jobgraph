package hicoden.jobgraph.fsm.runners

//
// The [[runtime]] is the placeholder for the functionality that occurs at
// runtime i.e. from the perspective of supporting all the functionality
// related to running the jobs
// @author Raymond Tay
// @version 1.0
//
package object runtime {

  import cats._, data._, implicits._
  import hicoden.jobgraph.{JobId, WorkflowId}
  import hicoden.jobgraph.configuration.engine.model.MesosConfig
  import hicoden.jobgraph.configuration.step.model.{Runner, JobConfig}

  // Represents the location of this "JobEngine"
  case class JobEngineLocationContext(hostname : String, port: Int)

  // This is pretty much the runtime representation of the job's configuration
  // in [[JobConfig]].
  case class JobContext(name: String, description: String, workdir: String, workflowId: WorkflowId, jobId: JobId, location: JobEngineLocationContext, runner: Runner)

  // This represents the contextual information needed for the job. Take note
  // that this abstraction gives us the ability to deploy jobs to different
  // Apache Mesos running in the data centers.
  //
  // @param taskExec /path/to/executable-mesos-scio-job
  // @param runAs the "user" that Apache Mesos will register this job with
  // @param jobCtx jobContext
  // @param mesosCtx contextual info regarding the Apache Mesos cluster
  case class MesosJobContext(taskExec : String, runAs: String, jobCtx: JobContext, mesosCtx: MesosConfig)

  // [[JobContextManifest]] is the typeclass where all functions related to
  // transforming [[JobConfig]] to [[JobContext]]es so that the runner can use
  // during the execution run of the Job s.t. [[JobConfig]] remains immutable
  // 
  trait JobContextManifest {
    def manifest(wfId: WorkflowId, jobId: JobId) =
      (cfg: JobConfig)⇒
        JobContext(name = cfg.name, description = cfg.description,
                   workdir = cfg.workdir, workflowId = wfId,
                   jobId = jobId, location = JobEngineLocationContext("0.0.0.0", 9000),
                   runner = cfg.runner)

    def manifestMesos(wfId: WorkflowId, jobId: JobId, mesosCfg: MesosConfig) =
      (cfg: JobConfig)⇒
        MesosJobContext(
          taskExec = "MesosScioJob",
          runAs = mesosCfg.runas,
          JobContext(name = cfg.name, description = cfg.description,
                   workdir = cfg.workdir, workflowId = wfId,
                   jobId = jobId, location = JobEngineLocationContext("0.0.0.0", 9000),
                   runner = cfg.runner), mesosCfg)
 
  }

  object JobContextManifest extends JobContextManifest

}
