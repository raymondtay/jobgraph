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

// queries for operations over the `workflow_template` table
trait WorkflowTemplates {

  /**
    * Takes the workflow configuration object and inserts a record into the
    * database table. Note: Database constraints are respected and client of
    * this call should handle the SQLException, if errors prevail.
    * @param jobConfig
    * @return ConnectionIO[Int] where number of records is indicated
    */
  def insertTemplate(wfConfig: WorkflowConfig) : ConnectionIO[Int] =
    sql"insert into workflow_template (id, name, description, jobgraph) values( ${wfConfig.id}, ${wfConfig.name}, ${wfConfig.description}, ${wfConfig.jobgraph} )".update.run

}

// queries for operations over the `job_template` table
trait JobTemplates extends FragmentFunctions {

  /**
    * Takes the job configuration object and inserts a record into the
    * database table. Note: Database constraints are respected and client of
    * this call should handle the SQLException, if errors prevail.
    * @param jobConfig
    * @return ConnectionIO[Int] where number of records is indicated
    */
  def insertTemplate(jobConfig: JobConfig) : ConnectionIO[Int] = {
    // TODO : replace using HLists
    val xs = runnerExpr.run(jobConfig.runner)
    val insertStatement =
      fr"insert into job_template (id, name, description, sessionid, restart, runner) values(" ++
      fr"${jobConfig.id}," ++
      fr"${jobConfig.name}," ++
      fr"${jobConfig.description}," ++
      fr"${jobConfig.sessionid}," ++
      fr"${jobConfig.restart.max}," ++ xs ++ fr")"
    insertStatement.update.run
  }

}

// Queries for operations over the `workflow_rt` & `job_rt` tables.
trait RuntimeDbOps extends FragmentFunctions {
  implicit val WorkflowStatesEnum = pgEnum(WorkflowStates, "WorkflowStates")
  implicit val JobStatesEnum = pgEnum(JobStates, "JobStates")

  /**
    * Inserts a workflow record into the `workflow_rt` table
    * @param wfTemplateId the configuration template id which we are suppose to
    * associate this runtime information with.
    * @param rec
    * @return a ConnectionIO[Int] object, when run indicates how many rows were
    * inserted.
    */
  def initWorkflowRecs : Reader[Workflow,ConnectionIO[Int]] = Reader{ (rec: Workflow) ⇒
    val insertWfStatement =
      fr"insert into workflow_rt(wf_id, wf_template_id, status, job_id) values(" ++
      fr"${rec.id}, " ++
      fr"${rec.config.id}, " ++
      fr"${rec.status}, " ++
      arrayUUIDExpr(rec.jobgraph.nodes.map(_.id).toList) ++
      fr");"
    (insertWfStatement +: rec.jobgraph.labNodes.map(n ⇒ initJobRec(n.vertex))).reduce(_ ++ _ ).update.run
  }

  /**
    * Inserts a job record into the `job_rt` table
    * @param rec
    * @return a ConnectionIO[Int] object, when run indicates how many rows were
    * inserted.
    */
  def initJobRec : Reader[Job, Fragment] = Reader{ (rec: Job) ⇒
    val insertStatement =
      fr"insert into job_rt (id, job_template_id, config, status) values(" ++
      fr"${rec.id}," ++
      fr"${rec.config.id}, " ++
      jobConfigExpr(rec.config) ++ fr"," ++
      fr"${JobStates.inactive}" ++
      fr");"
    insertStatement
  }

}

