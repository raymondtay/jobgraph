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

import hicoden.jobgraph.configuration.workflow.model._
import hicoden.jobgraph.configuration.step.model._


class WorkflowTemplatesSpecs extends Specification with ScalaCheck with WorkflowTemplates { def is = sequential ^ s2"""
  When a Workflow configuration is to be inserted into the database, it should return a Monadic object $insertOk
  """
  def insertOk = {
    val wfConfig = WorkflowConfig(id = 0, name = "Test Workflow", description = "Test Description", steps = Nil, jobgraph = List("0->1"))
    val r = insertTemplate(wfConfig)
    r must beAnInstanceOf[Free[ConnectionIO,_]]
    r must not beNull
  }
}

class JobTemplatesSpecs extends Specification with ScalaCheck with JobTemplates { def is = sequential ^ s2"""
  When a Job configuration is to be inserted into the database, it should return a Monadic object $insertOk
  """
  def insertOk = {
    val jobConfig =
      JobConfig(id = 0,
                name = "Test Workflow",
                description = "Test Description",
                workdir = "",
                sessionid = "blahblah",
                restart = Restart(1),
                runner = Runner("module.m", "path/to/execfile", Nil),
                inputs = Nil, outputs = Nil)
    val r = insertTemplate(jobConfig)
    r must beAnInstanceOf[Free[ConnectionIO,_]]
    r must not beNull
  }
}

