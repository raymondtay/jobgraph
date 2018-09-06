package hicoden.jobgraph.engine

/**
  * This package contains:
  * (a) JSON renderers using [[io.circe]] 
  * (b) Apache Beam job interpreter that is leveraged by [[JobCallbacks.scala]]
  *     to decipher what kind of status is returned by the remotely executing
  *     Jobgraph job.
  *
  * @author Raymond Tay
  * @version 1.0
  */
package object runtime {

  import hicoden.jobgraph.{JobStatus, JobStates, WorkflowStatus}
  import hicoden.jobgraph.configuration.step.model.{JobConfig, Restart, Runner}
  import hicoden.jobgraph.configuration.workflow.model.{WorkflowConfig, JobConfigOverrides, JobOverrides}

  import cats._, data._, implicits._
  import io.circe._, io.circe.generic.semiauto._, io.circe.parser._, io.circe.syntax._

  // one or more of the fields can be omitted with the exception of the field
  // `id`.
  implicit val jobOverridesDecoder : Decoder[JobOverrides] = new Decoder[JobOverrides] {
    final def apply(c: HCursor) : Decoder.Result[JobOverrides] = for {
      id            ← c.downField("id").as[Int]
      description   ← c.getOrElse("description")(none[String])
      workdir       ← c.getOrElse("workdir")(none[String])
      sessionid     ← c.getOrElse("sessionid")(none[String])
      timeout       ← c.getOrElse("timeout")(none[Int])
      runnerRunner  ← c.getOrElse("runnerRunner")(none[String])
      runnerCliargs ← c.getOrElse("runnerCliArgs")(none[List[String]])
    } yield JobOverrides(id, description, workdir, sessionid, timeout, runnerRunner, runnerCliargs)
  }

  implicit val jobConfigEncoder: Encoder[JobConfig] = deriveEncoder[JobConfig]
  implicit val runnerConfigEncoder: Encoder[Runner] = deriveEncoder[Runner]
  implicit val restartConfigEncoder: Encoder[Restart] = deriveEncoder[Restart]

  implicit val jobStatusEncoder: Encoder[JobStatus] = new Encoder[JobStatus] {
    final
    def apply(a: JobStatus) : Json = Json.obj(
      ("id", Json.fromString(a.id.toString)),
      ("status", Json.fromString(a.status.toString.toUpperCase))   
    )
  }

  implicit val wfStatusEncoder : Encoder[WorkflowStatus] = new Encoder[WorkflowStatus] {
    final
    def apply(a : WorkflowStatus) : Json = Json.obj(
      ("createTime", Json.fromString(a.createTime.toString)),
      ("status", Json.fromString(a.status.toString.toUpperCase)),
      ("steps", Json.arr(a.steps.map(jobStatusEncoder(_)):_*))
    )
  }

  implicit val workflowsEncoder : Encoder[List[WorkflowConfig]] = new Encoder[List[WorkflowConfig]] {
    final 
    def apply(xs: List[WorkflowConfig]) : Json = Json.obj(
      ("workflows", Json.arr(xs.map(workflowCfgEncoder(_)):_*))
    )
  }

  implicit val workflowCfgEncoder : Encoder[WorkflowConfig] =
    Encoder.forProduct3("id", "name", "description")(c ⇒ (c.id, c.name, c.description))

  // Java enums cannot be mapped directly into Scala so its being replicated -
  // There should not be any noticeable performance degradations.
  // See the documentation for [[org.apache.beam.sdk.PipelineResult.State]]
  object ApacheBeamJobStatuses extends Enumeration {
    type ApacheBeamJobStatus = Value
    val CANCELLED, DONE, FAILED, RUNNING, STOPPED, UNKNOWN, UPDATED = Value
  }
  
  // As jobgraph can potentially run other jobs other than Apache Beam, its not
  // necessary to perform a direct coupling to the job states in Apache Beam - in fact it should not as
  // we would be coupled too tightly to Apache Beam; therefore its a conscious
  // effort not to do so and instead build combinator that leverage a mapping of
  // our choice.
  trait ApacheBeamJobStatusFunctions {
    import org.apache.beam.sdk.PipelineResult
    import org.apache.beam.sdk.PipelineResult.State
    import cats._, data._, implicits._
  
    private val defaultMapping =
      Map(ApacheBeamJobStatuses.CANCELLED -> JobStates.forced_termination,
          ApacheBeamJobStatuses.DONE      -> JobStates.finished,
          ApacheBeamJobStatuses.FAILED    -> JobStates.failed,
          ApacheBeamJobStatuses.RUNNING   -> JobStates.active,
          ApacheBeamJobStatuses.UNKNOWN   -> JobStates.unknown,
          ApacheBeamJobStatuses.UPDATED   -> JobStates.updated,
          ApacheBeamJobStatuses.STOPPED   -> JobStates.inactive
      )
  
    def decipherBeamJobStatus = Reader{ (status: String) ⇒ scala.util.Try{ ApacheBeamJobStatuses.withName(status.toUpperCase) }.toOption }
  
    def translate = Reader{ (status : ApacheBeamJobStatuses.ApacheBeamJobStatus) ⇒ defaultMapping.get(status).fold(none[JobStates.States])(_.some) }
  
    def lookup = Reader { (status: String) ⇒ decipherBeamJobStatus(status).fold(none[JobStates.States])(translate(_)) }
  }
  object ApacheBeamJobStatusFunctions extends ApacheBeamJobStatusFunctions
}

