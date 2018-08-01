package hicoden.jobgraph.engine

/**
  * This package contains mostly of the implicits that happened to be JSON
  * renderers using [[io.circe]] 
  * 
  * @author Raymond Tay
  * @version 1.0
  */
package object runtime {

  import hicoden.jobgraph.{JobStatus, WorkflowStatus}
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
      runnerRunner  ← c.getOrElse("runnerRunner")(none[String])
      runnerCliargs ← c.getOrElse("runnerCliArgs")(none[List[String]])
      toPersist     ← c.getOrElse("persist")(false)
    } yield JobOverrides(id, description, workdir, sessionid, runnerRunner, runnerCliargs, toPersist)
  }

  //implicit val jobConfigOverridesDecoder
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
}

