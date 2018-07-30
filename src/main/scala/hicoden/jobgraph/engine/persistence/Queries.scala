package hicoden.jobgraph.engine.persistence

import doobie._
import doobie.implicits._
import doobie.postgres._
import doobie.postgres.implicits._
import cats._
import cats.data._
import cats.effect.IO
import cats.implicits._

import hicoden.jobgraph._
import hicoden.jobgraph.configuration.workflow.model._
import hicoden.jobgraph.configuration.step.model._

// Queries for operations over the `workflow_rt` , `job_rt`,
// `workflow_template` & `job_template` tables.
trait DatabaseOps extends FragmentFunctions {

  // postgresql enum types implicits
  implicit val WorkflowStatesEnum = pgEnum(WorkflowStates, "WorkflowStates")
  implicit val JobStatesEnum      = pgEnum(JobStates, "JobStates")

  /**
    * Takes the workflow configuration object and prepares the data for the
    * database table. Note: Database constraints are respected and client of
    * this call should handle the SQLException, if errors prevail.
    * @param jobConfig
    * @return ConnectionIO[Int] where number of records is indicated
    */
  def workflowConfigOp(wfConfig: WorkflowConfig) : Fragment =
    fr"insert into workflow_template (id, name, description, jobgraph) values( ${wfConfig.id}, ${wfConfig.name}, ${wfConfig.description}, ${wfConfig.jobgraph} );"

  /**
    * Takes the job configuration object and prepares the data for the
    * database table. Note: Database constraints are respected and client of
    * this call should handle the SQLException, if errors prevail.
    * @param jobConfig
    * @return ConnectionIO[Int] where number of records is indicated
    */
  def jobConfigOp(jobConfig: JobConfig) : Fragment = {
    // TODO : replace using HLists
    val xs = runnerExpr.run(jobConfig.runner)
    val insertStatement =
      fr"insert into job_template (id, name, description, sessionid, restart, runner) values(" ++
      fr"${jobConfig.id}," ++
      fr"${jobConfig.name}," ++
      fr"${jobConfig.description}," ++
      fr"${jobConfig.sessionid}," ++
      fr"${jobConfig.restart.max}," ++ xs ++ fr");"
    insertStatement
  }

  /**
    * Inserts a workflow record into the `workflow_rt` table
    * @param wfTemplateId the configuration template id which we are suppose to
    * associate this runtime information with.
    * @param rec
    * @return a ConnectionIO[Int] object, when run indicates how many rows were
    * inserted.
    */
  def workflowRtOp : Reader[Workflow,ConnectionIO[Int]] = Reader{ (rec: Workflow) ⇒
    val insertWfStatement =
      fr"insert into workflow_rt(wf_id, wf_template_id, status, job_id) values(" ++
      fr"${rec.id}, " ++
      fr"${rec.config.id}, " ++
      fr"${rec.status}, " ++
      arrayUUIDExpr(rec.jobgraph.nodes.map(_.id).toList) ++
      fr");"
    (insertWfStatement +: rec.jobgraph.labNodes.map(n ⇒ jobRtOp(n.vertex))).reduce(_ ++ _ ).update.run
  }

  /**
    * Inserts a job record into the `job_rt` table
    * @param rec
    * @return a ConnectionIO[Int] object, when run indicates how many rows were
    * inserted.
    */
  def jobRtOp : Reader[Job, Fragment] = Reader{ (rec: Job) ⇒
    val insertStatement =
      fr"insert into job_rt (id, job_template_id, config, status) values(" ++
      fr"${rec.id}," ++
      fr"${rec.config.id}, " ++
      jobConfigExpr(rec.config) ++ fr"," ++
      fr"${JobStates.inactive}" ++
      fr");"
    insertStatement
  }

  def deleteAllWorkflowTemplates = sql"delete from workflow_template".update
  def deleteAllJobTemplates = sql"delete from job_template".update

}

