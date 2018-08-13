package hicoden.jobgraph.engine.persistence

import org.specs2.{ScalaCheck, Specification}
import org.specs2._

import doobie._
import doobie.implicits._
import cats._
import cats.free.Free
import cats.data._
import cats.effect.IO
import cats.implicits._

import hicoden.jobgraph._
import hicoden.jobgraph.configuration.workflow.model._
import hicoden.jobgraph.configuration.step.model._


object WorkflowDAG {
  import quiver._

  val jobA =
    Job("job-a", JobConfig(1, "job-a-config", "test", "/tmp", "", 1, Restart(1), Runner("module.m", "/path/to/execfile", "--arg1=value71"::Nil)))
  val jobB =
    Job("job-b", JobConfig(2, "job-b-config", "test", "/tmp", "", 1, Restart(1), Runner("module.m", "/path/to/execfile", "--arg1=value72"::Nil)))
  val jobC =
    Job("job-c", JobConfig(3, "job-c-config", "test", "/tmp", "", 1, Restart(1), Runner("module.m", "/path/to/execfile", "--arg1=value73"::Nil)))

  val nodes =
    LNode(jobA, jobA.id) ::
    LNode(jobB, jobB.id) ::
    LNode(jobC, jobC.id) :: Nil

  val edges =
    LEdge(jobA, jobB, "1 -> 2") ::
    LEdge(jobA, jobC, "2 -> 3") :: Nil

  val wfConfig = WorkflowConfig(id = 42, "Sample", "A simple demonstration", "1->2"::"2->3"::Nil)
  def graphGen : Workflow = WorkflowOps.createWf(wfConfig.some, nodes.to[scala.collection.immutable.Seq])(edges.to[scala.collection.immutable.Seq])
}

class DbOpsSpecs extends Specification with ScalaCheck with DatabaseOps { def is = sequential ^ s2"""
  When a Workflow configuration is to be inserted into the database, it should return a Monadic object $dbOpToInsertWorkflowTemplate
  When a Job configuration is to be inserted into the database, it should return a Monadic object $dbOpToInsertJobTemplate
  When a workflow and its DAG records are about to be inserted into the database, it should return a Monadic object $insertWfDAGOk
  """

  def dbOpToInsertWorkflowTemplate = {
    val wfConfig = WorkflowConfig(id = 0, name = "Test Workflow", description = "Test Description", jobgraph = List("0->1"))
    val r = workflowConfigOp(wfConfig)
    r must beAnInstanceOf[Free[ConnectionIO,_]]
    r must not beNull
  }

  def dbOpToInsertJobTemplate = {
    val jobConfig =
      JobConfig(id = 0,
                name = "Test Workflow",
                description = "Test Description",
                workdir = "",
                sessionid = "blahblah",
                timeout = 1,
                restart = Restart(1),
                runner = Runner("module.m", "path/to/execfile", Nil))
    val r = jobConfigOp(jobConfig)
    r must beAnInstanceOf[Free[ConnectionIO,_]]
    r must not beNull
  }

  def insertWfDAGOk = {
    // Generates a sample workflow
    val wf = WorkflowDAG.graphGen
    workflowRtOp(wf) must beAnInstanceOf[Fragment] // take note we are not actually performing a database insert here.
  }

}

