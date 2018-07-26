# How to programmatically insert a workflow and job configuration into the Postgresql database 

```tut
import hicoden.jobgraph._
import hicoden.jobgraph.engine.persistence._
import hicoden.jobgraph.configuration.step.model._
import hicoden.jobgraph.configuration.workflow.model._
import doobie._, implicits._

val jobConfigs =
  JobConfig(1, "job-a-config", "test", "/tmp", "", Restart(1), Runner("module.m", "/path/to/execfile", "--arg1=value71"::Nil), Nil, Nil) :: JobConfig(2, "job-b-config", "test", "/tmp", "", Restart(1), Runner("module.m", "/path/to/execfile", "--arg1=value72"::Nil), Nil, Nil) :: JobConfig(3, "job-c-config", "test", "/tmp", "", Restart(1), Runner("module.m", "/path/to/execfile", "--arg1=value73"::Nil), Nil, Nil) :: Nil

val j = new JobTemplates{}
val w = new WorkflowTemplates{}
jobConfigs.map(jobConfig => j.insertTemplate(jobConfig).transact(Transactors.xa).unsafeRunSync)
val wfConfig = WorkflowConfig(id = 42, "Sample", "A simple demonstration", Nil, "1->2"::"2->3"::Nil)
w.insertTemplate(wfConfig).transact(Transactors.xa).unsafeRunSync
```

# How to programmatically insert a job record into the Postgresql database

The table `job_rt` contains records which relates to the execution of the job
and includes information like the runtime job id assigned by `jobgraph`, status
of the execution. Each job allows its configuration to be overrided during
run-time and this information is stored in another database object see
[[JobConfigRT]].

An example is shown below but you do not operate directly like this.
```tut
import hicoden.jobgraph._
import hicoden.jobgraph.engine.persistence._
import hicoden.jobgraph.configuration.step.model._
import doobie._, implicits._

// Sample job config
val jConfig = JobConfig(1,"A sample job template", "A sample job template description", "/tmp", "xoxp-ASDASD-123", Restart(2), Runner("module.x", "/path/to/execfile", "--arg1=value1"::Nil), Nil, Nil)

// Create the database operator
val j = new RuntimeDbOps{}

// What happens behind the scenes is that the `xa` object is lazily evaluated
// and instantiated and allows developer to insert a data record
j.initJobRec(Job("a test job", jConfig)).update.run.transact(Transactors.xa).unsafeRunSync
```

# How to programmatically insert a Workflow (and its DAG) record into the Postgresql database

```tut
import hicoden.jobgraph._
import hicoden.jobgraph.configuration.workflow.model._
import hicoden.jobgraph.engine.persistence._
import doobie._, implicits._

object GraphData {
  import quiver._

  val jobA = Job("job-a", JobConfig(1, "job-a-config", "test", "/tmp", "", Restart(1), Runner("module.m", "/path/to/execfile", "--arg1=value71"::Nil), Nil, Nil))
  val jobB = Job("job-b", JobConfig(2, "job-b-config", "test", "/tmp", "", Restart(1), Runner("module.m", "/path/to/execfile", "--arg1=value72"::Nil), Nil, Nil))
  val jobC = Job("job-c", JobConfig(3, "job-c-config", "test", "/tmp", "", Restart(1), Runner("module.m", "/path/to/execfile", "--arg1=value73"::Nil), Nil, Nil))

  val nodes =
    LNode(jobA, jobA.id) ::
    LNode(jobB, jobB.id) ::
    LNode(jobC, jobC.id) :: Nil

  val edges =
    LEdge(jobA, jobB, "1 -> 2") ::
    LEdge(jobA, jobC, "2 -> 3") :: Nil

  val wfConfig = WorkflowConfig(id = 42, "Sample", "A simple demonstration", Nil, "1->2"::"2->3"::Nil)
  def graphGen : Workflow = WorkflowOps.createWf(nodes.to[scala.collection.immutable.Seq])(edges.to[scala.collection.immutable.Seq]).copy(config = wfConfig)
}

// Generates the workflow in question
val wf = GraphData.graphGen
val dbOps = new RuntimeDbOps{}

// If the template id you indicate here is not available in the database, then
// you will experience a runtime SQLException
dbOps.initWorkflowRecs(wf).transact(Transactors.xa).unsafeRunSync

// If the following expression is uncommented, you will immediately experience
// constraint violations which is what we want and data integrity is
// maintained.
dbOps.initWorkflowRecs(wf).transact(Transactors.xa).unsafeRunSync
```
