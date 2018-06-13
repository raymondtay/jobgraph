package hicoden

// Type abbreviations, type synonyms etc
//
package object jobgraph {

  import java.util.UUID

  type WorkflowId = UUID
  type JobId = UUID

  case class JobStatus(id: JobId, status: JobStates.States)
  case class WorkflowStatus(createTime: java.time.Instant, status: WorkflowStates.States, steps: List[JobStatus])

}
