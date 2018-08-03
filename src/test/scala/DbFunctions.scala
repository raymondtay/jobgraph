package hicoden.jobgraph.engine.persistence

// A house for a small collection of utilities for db operations.
object DbFunctions {

  import doobie._
  import doobie.implicits._
  import cats._
  import cats.data._
  import cats.effect.IO
  import cats.implicits._
  import scala.concurrent.duration._

  private val dropJobRt = sql"delete from job_rt"
  private val dropWfRt = sql"delete from workflow_rt"
  private val dropJobTemplate = sql"delete from job_template"
  private def dropJobTemplateWhere(idx: Int) = sql"delete from job_template where id = $idx"
  private val dropWfTemplate = sql"delete from workflow_template"
  private def dropWfTemplateWhere(idx: Int) = sql"delete from workflow_template where id = $idx"

  /**
    * Clear the data in the [[job_rt]] and [[workflow_rt]] tables.
    * @param transactor - pass in your own transactor
    */
  def clearJobsRuntimeData(tx: Transactor[IO]) =
    (dropJobRt.update.run *> dropWfRt.update.run).transact(tx).unsafeRunTimed(1.seconds)

  /**
    * Clear the data in the [[job_template]] and [[workflow_template]] tables.
    * @param transactor - pass in your own transactor
    */
  def clearConfigData(tx: Transactor[IO]) =
    (dropJobTemplate.update.run *> dropWfTemplate.update.run).transact(tx).unsafeRunTimed(1.seconds)

  /**
    * Clear all data in the [[job_rt]], [[workflow_rt]], [[job_template]]
    * and [[workflow_template]] in that order
    * @param transactor - pass in your own transactor
    */
  def clearAllData(tx: Transactor[IO]) =
    (dropJobRt.update.run       *>
     dropWfRt.update.run        *>
     dropJobTemplate.update.run *>
     dropWfTemplate.update.run   ).transact(tx).unsafeRunSync

  def clearJobTemplateWhere(id : Int, tx: Transactor[IO]) = dropJobTemplateWhere(id).update.run.transact(tx).unsafeRunTimed(1.seconds)
  def clearWfTemplateWhere(id : Int, tx: Transactor[IO]) = dropWfTemplateWhere(id).update.run.transact(tx).unsafeRunTimed(1.seconds)

}
