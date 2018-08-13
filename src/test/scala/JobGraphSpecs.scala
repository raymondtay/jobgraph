package hicoden.jobgraph

import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary._
import Gen.{containerOfN, choose, pick, mapOf, listOf, listOfN, oneOf}
import Prop.{forAll, throws, AnyOperators}

/**
  * This data here generates the classical scatter-gather digraph involving 4
  * imaginary job nodes a,b,c,d.
  */
object GraphDataScenarioA {
  import quiver._

  val jobA = Job("job-a")
  val jobB = Job("job-b")
  val jobC = Job("job-c")
  val jobD = Job("job-d")

  val nodes =
    LNode(jobA, jobA.id) ::
    LNode(jobB, jobB.id) ::
    LNode(jobC, jobC.id) ::
    LNode(jobD, jobD.id) :: Nil

  val edges =
    LEdge(jobA, jobB, "a -> b") ::
    LEdge(jobA, jobC, "a -> c") ::
    LEdge(jobC, jobD, "c -> d") ::
    LEdge(jobB, jobD, "b -> d") :: Nil

  def graphGenA : Workflow = WorkflowOps.createWf(None, nodes)(edges)

  def workflowGenA : Gen[Workflow] = for {
    workflow ← oneOf(graphGenA :: Nil)
  } yield {
    workflow
  }

  implicit val workflowArbGeneratorA = Arbitrary(workflowGenA)
}

/**
  * This data here generates the classical scatter-gather digraph involving 6
  * imaginary job nodes a,b,c,d,e,f in 3 fictitious workflows
  */
object GraphDataScenarioB {
  import quiver._

  val jobA = Job("job-a")
  val jobB = Job("job-b")
  val jobC = Job("job-c")
  val jobD = Job("job-d")
  val jobE = Job("job-e")
  val jobF = Job("job-f")

  // see [[edgesA]] for the topology
  val nodesA =
    LNode(jobA, jobA.id) ::
    LNode(jobB, jobB.id) ::
    LNode(jobC, jobC.id) :: Nil

  // see [[edgesB]] for the topology
  val nodesB =
    LNode(jobA, jobA.id) ::
    LNode(jobB, jobB.id) ::
    LNode(jobC, jobC.id) ::
    LNode(jobD, jobD.id) :: Nil

  // see [[edgesC]] for the topology
  val nodesC =
    LNode(jobA, jobA.id) ::
    LNode(jobB, jobB.id) ::
    LNode(jobC, jobC.id) ::
    LNode(jobD, jobD.id) ::
    LNode(jobE, jobE.id) ::
    LNode(jobF, jobF.id) :: Nil

  val edgesA =
    LEdge(jobA, jobB, "a -> b") ::
    LEdge(jobA, jobC, "a -> c") :: Nil

  val edgesB =
    LEdge(jobA, jobB, "a -> b") ::
    LEdge(jobA, jobC, "a -> c") ::
    LEdge(jobC, jobD, "c -> d") ::
    LEdge(jobB, jobD, "b -> d") :: Nil

  val edgesC =
    LEdge(jobA, jobB, "a -> b") ::
    LEdge(jobA, jobC, "a -> c") ::
    LEdge(jobC, jobD, "c -> d") ::
    LEdge(jobB, jobD, "b -> d") ::
    LEdge(jobA, jobE, "a -> e") ::
    LEdge(jobA, jobF, "a -> f") :: Nil

  def graphGenB : List[Workflow] = List(
    WorkflowOps.createWf(None, nodesA)(edgesA),
    WorkflowOps.createWf(None, nodesB)(edgesB),
    WorkflowOps.createWf(None, nodesC)(edgesC)
  )

  def workflowGenB : Gen[Workflow] = for {
    workflow ← oneOf(graphGenB)
  } yield {
    workflow
  }

  implicit val workflowArbGeneratorB = Arbitrary(workflowGenB)
}

/**
  * This data here generates the classical scatter-gather digraph involving 6
  * imaginary job nodes a,b,c,d,e,f in 3 fictitious workflows. Be aware that
  * the test data is used for testing of discovering "the next jobs to start"
  */
object GraphDataScenarioC {
  import quiver._

  val jobA = Job("job-a")
  val jobB = Job("job-b")
  val jobC = Job("job-c")
  val jobD = Job("job-d")
  val jobE = Job("job-e")
  val jobF = Job("job-f")

  // see [[edgesA]] for the topology
  val nodesA =
    LNode(jobA, jobA.id) ::
    LNode(jobB, jobB.id) ::
    LNode(jobC, jobC.id) :: Nil

  // see [[edgesB]] for the topology
  val nodesB =
    LNode(jobA, jobA.id) ::
    LNode(jobB, jobB.id) ::
    LNode(jobC, jobC.id) ::
    LNode(jobD, jobD.id) :: Nil

  // see [[edgesC]] for the topology
  val nodesC =
    LNode(jobA, jobA.id) ::
    LNode(jobB, jobB.id) ::
    LNode(jobC, jobC.id) ::
    LNode(jobD, jobD.id) ::
    LNode(jobE, jobE.id) ::
    LNode(jobF, jobF.id) :: Nil

  val edgesA =
    LEdge(jobA, jobB, "a -> b") ::
    LEdge(jobA, jobC, "a -> c") :: Nil

  val edgesB =
    LEdge(jobA, jobB, "a -> b") ::
    LEdge(jobA, jobC, "a -> c") ::
    LEdge(jobC, jobD, "c -> d") ::
    LEdge(jobB, jobD, "b -> d") :: Nil

  val edgesC =
    LEdge(jobA, jobB, "a -> b") ::
    LEdge(jobA, jobC, "a -> c") ::
    LEdge(jobC, jobD, "c -> d") ::
    LEdge(jobB, jobD, "b -> d") ::
    LEdge(jobA, jobE, "a -> e") ::
    LEdge(jobA, jobF, "a -> f") :: Nil

  // Purely for generation of the [a -> b, a -> c]
  def graphGenUseCase1 : Workflow = WorkflowOps.createWf(None, nodesA)(edgesA)

  // Purely for generation of the [a -> b, a -> c, c -> dNone, n b -> d]
  def graphGenUseCase2 : Workflow = WorkflowOps.createWf(None, nodesB)(edgesB)

  def graphGenUseCase3 : Workflow = WorkflowOps.createWf(None, nodesC)(edgesC)

  def workflowUseCase1Gen : Gen[Workflow] = for {
    workflow ← oneOf(graphGenUseCase1 :: Nil)
  } yield {
    workflow
  }

  def workflowUseCase2Gen : Gen[Workflow] = for {
    workflow ← oneOf(graphGenUseCase2 :: Nil)
  } yield {
    workflow
  }

  def workflowUseCase3Gen : Gen[Workflow] = for {
    workflow ← oneOf(graphGenUseCase3 :: Nil)
  } yield {
    workflow
  }

  implicit val workflowUseCase1ArbGenerator = Arbitrary(workflowUseCase1Gen)
  implicit val workflowUseCase2ArbGenerator = Arbitrary(workflowUseCase2Gen)
  implicit val workflowUseCase3ArbGenerator = Arbitrary(workflowUseCase3Gen)
}

// This graph is from the CLRS Algorithm textbook page 613 where Professor
// Bumstead needs to figure how what he needs to do (in sequence) so that he
// can get his day started; run the [[Engine]]
object GraphDataScenarioD {
  
  import quiver._

  val shirtJob = Job("shirt")
  val tieJob = Job("tie")
  val jacketJob = Job("jacket")
  val beltJob = Job("belt")
  val pantsJob = Job("pants")
  val undershortsJob = Job("undershorts")
  val socksJob = Job("socks")
  val shoesJob = Job("shoes")

  val nodes =
    LNode(shirtJob, shirtJob.id) ::
    LNode(tieJob, tieJob.id) ::
    LNode(jacketJob, jacketJob.id) ::
    LNode(beltJob, beltJob.id) ::
    LNode(pantsJob, pantsJob.id) ::
    LNode(undershortsJob, undershortsJob.id) ::
    LNode(socksJob, socksJob.id) ::
    LNode(shoesJob, shoesJob.id) :: Nil

  val edges =
    LEdge(shirtJob,tieJob,"shirt->tie") ::
    LEdge(shirtJob,beltJob, "shirt->belt") ::
    LEdge(tieJob,jacketJob,"tie->jacket") ::
    LEdge(beltJob,jacketJob,"belt->jacket") ::
    LEdge(pantsJob,beltJob,"pants->belt") ::
    LEdge(undershortsJob,pantsJob,"undershorts->pants") ::
    LEdge(undershortsJob,shoesJob,"undershorts->shoes") ::
    LEdge(pantsJob,shoesJob,"pants->shoes") ::
    LEdge(socksJob,shoesJob,"socks->shoes") :: Nil
  
  def graphGenUseCase =
    WorkflowOps.createWf(None, nodes)(edges)

  def workflowUseCaseGen : Gen[Workflow] = for {
    workflow ← oneOf(graphGenUseCase :: Nil)
  } yield {
    workflow
  }

  implicit val workflowUseCaseArbGenerator = Arbitrary(workflowUseCaseGen)
}

/**
 * Specifications for the workflows and the category of tests conducted here
 * are related to the following:
 *
 * + Creating workflows
 * + Updating workflows
 * + Starting workflows
 * + Stopping workflows
 *
 * Note: Using code + branch coverage, we cover as much as we possibly can.
 *
 * At the moment, the multigraph library we are leveraging offers us the
 * capabilities to do all kinds of interesting graph problems like Spanning
 * Trees but we do not provide any test coverage for those as we don't use
 * them, yet.
 *
 *
 * @author Raymond Tay
 * @version 1.0
 */
class JobGraphSpecs extends mutable.Specification with ScalaCheck {
  import quiver._

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 1
  import cats._, data._, implicits._, Validated._

  {
    import GraphDataScenarioA.workflowArbGeneratorA
    "JobGraphs, when created, would have a non-empty job graph and the create_timestamp is always earlier than the current timestamp" >> prop { (workflow: Workflow) ⇒
      assert(workflow.create_timestamp isBefore java.time.Instant.now().plusNanos(3))
      workflow.jobgraph.countNodes must be_>(0)
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioA.{workflowArbGeneratorA}
    "Updating of ANY job of its state in the created workflow would be respected" >> prop { (workflow: Workflow) ⇒
      assert(workflow.create_timestamp isBefore java.time.Instant.now().plusNanos(3))
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioA.jobD.id)(JobStates.forced_termination)
      workflow.jobgraph.countNodes must be_>(0)
      workflow.jobgraph.bfs(GraphDataScenarioA.jobD).head.state must be_==(JobStates.forced_termination)
      WorkflowOps.getWorkflowStatus(workflow.id) must beSome
      WorkflowOps.getWorkflowStatus(java.util.UUID.randomUUID) must beNone
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  // Tests for updating the workflow
  //
  {
    import GraphDataScenarioA.{workflowArbGeneratorA}
    "Updating of ALL jobs of their respective state in the created workflow would be respected" >> prop { (workflow: Workflow) ⇒
      assert(workflow.create_timestamp isBefore java.time.Instant.now().plusNanos(3))
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioA.jobA.id)(JobStates.start)
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioA.jobB.id)(JobStates.active)
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioA.jobC.id)(JobStates.finished)
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioA.jobD.id)(JobStates.forced_termination)
      WorkflowOps.isWorkflowCompleted(workflow.id) must be_==(false)
      WorkflowOps.isWorkflowForcedStop(workflow.id) must be_==(true) // since jobD ∈ forced_termination
      workflow.jobgraph.countNodes must be_>(0)
      workflow.jobgraph.bfs(GraphDataScenarioA.jobA).head.state must be_==(JobStates.start)
      workflow.jobgraph.bfs(GraphDataScenarioA.jobB).head.state must be_==(JobStates.active)
      workflow.jobgraph.bfs(GraphDataScenarioA.jobC).head.state must be_==(JobStates.finished)
      workflow.jobgraph.bfs(GraphDataScenarioA.jobD).head.state must be_==(JobStates.forced_termination)
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioB.{workflowArbGeneratorB}
    "Starting the workflow is equivalent to setting their states to 'start'" >> prop { (workflow: Workflow) ⇒
      val startNodes = WorkflowOps.startWorkflow(workflow.id)
      startNodes must beSome((nodes: Set[Job]) ⇒ nodes must not be empty)
      startNodes must beSome((nodes: Set[Job]) ⇒ nodes.map(_.id) must be_==(workflow.jobgraph.roots.map(_.id)))
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioB.{workflowArbGeneratorB}
    "Starting ANY workflow with an invalid workflow identifier is an error and a None value is returned" >> prop { (workflow: Workflow) ⇒
      import quiver.{empty ⇒ emptyGraph}
      val fakeWorkflow = Workflow( emptyGraph[Job, JobId, String] ) // workflow with an empty job-graph, this is not allowed
      WorkflowOps.startWorkflow(fakeWorkflow.id) must beNone
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioB.{workflowArbGeneratorB}
    "Stopping the workflow is equivalent to setting their states to 'forced_termination'" >> prop { (workflow: Workflow) ⇒
      WorkflowOps.stopWorkflow(workflow.id) must beRight
      workflow.jobgraph.nodes.map(n ⇒ n.state must be_==(JobStates.forced_termination))
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioB.{workflowArbGeneratorB}
    "Stopping an workflow with an invalid workflow-id is an error and will be caught as a Either.Left value" >> prop { (workflow: Workflow) ⇒
      WorkflowOps.stopWorkflow(java.util.UUID.randomUUID) must beLeft
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioB.{workflowArbGeneratorB}
    "Attempting to stop a workflow with an invalid workflow identifier (i.e. not recognized by the systems) will give errors and returned as a Either.Left value" >> prop { (workflow: Workflow) ⇒
      import quiver.{empty ⇒ emptyGraph}
      val fakeWorkflow = Workflow( emptyGraph[Job, JobId, String] )
      val fakeJob = Job("fake-job")
      WorkflowOps.updateWorkflow(fakeWorkflow.id)(fakeJob.id)(JobStates.start) must beLeft
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioB.{workflowArbGeneratorB}
    "Attempting to update a workflow with a job that is not associated with it will give errors can caught and returned as a Either.Left value" >> prop { (workflow: Workflow) ⇒
      val fakeJob = Job("fake-job")
      WorkflowOps.updateWorkflow(workflow.id)(fakeJob.id)(JobStates.start) must beLeft
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  // Tests for discovering the "next" nodes to trigger the workflow
  //
  {
    import GraphDataScenarioC.{workflowUseCase3ArbGenerator}
    "Attempting to discover the 'next' nodes to start with an invalid workflow identifier is an error" >> prop { (workflow: Workflow) ⇒
      import quiver.{empty ⇒ emptyGraph}
      val fakeWorkflow = Workflow(emptyGraph[Job, JobId, String])
      val job = Job("fake-job")
      WorkflowOps.discoverNextJobsToStart(fakeWorkflow.id)(job.id) must beLeft{
        (errorString:String) ⇒ errorString must beEqualTo(s"Cannot discover workflow of the id: ${fakeWorkflow.id}")}
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioC.{workflowUseCase3ArbGenerator}
    "Attempting to discover the 'next' nodes to start with an valid workflow identifier but invalid job identifier is an logical error, and a empty container is returned." >> prop { (workflow: Workflow) ⇒
      import quiver.{empty ⇒ emptyGraph}
      val job = Job("fake-job")
      WorkflowOps.discoverNextJobsToStart(workflow.id)(job.id) must beRight{
        (jobNodes:Vector[Job]) ⇒ jobNodes must be empty
      }
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioC.{workflowUseCase3ArbGenerator}
    "At the beginning where no workflows are started (though created), the next nodes would be equivalent to the 'root' of the jobgraph" >> prop { (workflow: Workflow) ⇒
      val nodes = workflow.jobgraph.roots.map(root ⇒ WorkflowOps.discoverNextJobsToStart(workflow.id)(root.id))
      nodes must not be empty
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioC.{workflowUseCase1ArbGenerator}
    "SIMULATION: graph [a -> b, a -> c] " >> prop { (workflow: Workflow) ⇒
      val startNodes = WorkflowOps.startWorkflow(workflow.id) // the workflow has "started"
      startNodes must beSome((nodes: Set[Job]) ⇒ nodes must not be empty)
      startNodes must beSome((nodes: Set[Job]) ⇒ nodes must be_==(workflow.jobgraph.roots))

      // simulate the first node has completed, successfully.
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioC.jobA.id)(JobStates.finished)
      WorkflowOps.discoverNext(workflow.id)(GraphDataScenarioC.jobA.id) must beRight {
        (jobNodes: Vector[Job]) ⇒
          jobNodes.size must be_==(2)
          jobNodes must contain(be_==(GraphDataScenarioC.jobB), be_==(GraphDataScenarioC.jobC)) // Make further assertions about what node should be there
      }

      // simulate the second node has completed, successfully.
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioC.jobB.id)(JobStates.finished)
      WorkflowOps.discoverNext(workflow.id)(GraphDataScenarioC.jobB.id) must beRight {
        (jobNodes: Vector[Job]) ⇒ jobNodes.size must be_==(0)
      }

      // simulate the last node has completed, successfully.
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioC.jobC.id)(JobStates.finished)
      WorkflowOps.discoverNext(workflow.id)(GraphDataScenarioC.jobC.id) must beRight {
        (jobNodes: Vector[Job]) ⇒ jobNodes.size must be_==(0)
      }

      WorkflowOps.getWorkflowStatus(workflow.id) must beSome
      WorkflowOps.getWorkflowStatus(java.util.UUID.randomUUID) must beNone

      // Reset it for the next subsequent test run
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioC.jobA.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioC.jobB.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(GraphDataScenarioC.jobC.id)(JobStates.inactive)

      ok
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }

  {
    import GraphDataScenarioC.{workflowUseCase2ArbGenerator}
    "SIMULATION: graph [a -> b, a -> c, c -> d, b -> d] " >> prop { (workflow: Workflow) ⇒
      import GraphDataScenarioC.{jobA, jobB, jobC, jobD}
      val startNodes = WorkflowOps.startWorkflow(workflow.id) // the workflow has "started" i.e. Job-A ∈ START
      startNodes must beSome((nodes: Set[Job]) ⇒ nodes must not be empty)
      startNodes must beSome((nodes: Set[Job]) ⇒ nodes must be_==(workflow.jobgraph.roots))

      // simulate the first node has completed, successfully. i.e. Job-A ∈ FINISHED
      WorkflowOps.updateWorkflow(workflow.id)(jobA.id)(JobStates.finished)
      WorkflowOps.discoverNext(workflow.id)(jobA.id) must beRight {
        (jobNodes: Vector[Job]) ⇒
          jobNodes.size must be_==(2)
          jobNodes must contain(be_==(jobB), be_==(jobC)) // Make further assertions about what node should be there
      }

      WorkflowOps.discoverNext(workflow.id)(jobD.id) must beRight {
        (jobNodes: Vector[Job]) ⇒ jobNodes.size must be_==(0)
      }

      // simulate the second node has completed, successfully.
      WorkflowOps.updateWorkflow(workflow.id)(jobB.id)(JobStates.finished)
      WorkflowOps.discoverNext(workflow.id)(jobB.id) must beRight {
        (jobNodes: Vector[Job]) ⇒
          jobNodes.size must be_==(0)
      }
 
      // simulate the last node has completed, successfully.
      WorkflowOps.updateWorkflow(workflow.id)(jobC.id)(JobStates.finished)
      WorkflowOps.discoverNext(workflow.id)(jobC.id) must beRight {
        (jobNodes: Vector[Job]) ⇒
          jobNodes.size must be_==(1)
          jobNodes must contain(be_==(jobD)) // Make further assertions about what node should be there
      }
 
      WorkflowOps.getWorkflowStatus(workflow.id) must beSome
      WorkflowOps.getWorkflowStatus(java.util.UUID.randomUUID) must beNone

      // Reset it for the next subsequent test run
      WorkflowOps.updateWorkflow(workflow.id)(jobA.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(jobB.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(jobC.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(jobD.id)(JobStates.inactive)

      ok
    }.set(minTestsOk = minimumNumberOfTests, workers = 1)
  }
}

/*
class JobGraphSpecs2 extends org.specs2.mutable.Specification with ScalaCheck {
  import quiver._
  import cats._, data._, implicits._

  sequential // all specifications are run sequentially

  val minimumNumberOfTests = 1
  import cats._, data._, implicits._, Validated._

  {
    import GraphDataScenarioD.{socksJob, undershortsJob, shirtJob, pantsJob, tieJob, beltJob, jacketJob, shoesJob, workflowUseCaseArbGenerator}
    "SIMULATION of Professor Bumstead problem." >> prop { (workflow: Workflow) ⇒
println(workflow)
      val startNodes = WorkflowOps.startWorkflow(workflow.id) // the workflow has "started" i.e. Job-A ∈ START
      startNodes must beSome((nodes: Set[Job]) ⇒ nodes must not be empty)
      startNodes must beSome((nodes: Set[Job]) ⇒ nodes.map(_.id) must be_==(workflow.jobgraph.roots.map(_.id)))

      // simulate that all start nodes have completed successfully
      WorkflowOps.updateWorkflow(workflow.id)(socksJob.id)(JobStates.finished)
      WorkflowOps.updateWorkflow(workflow.id)(undershortsJob.id)(JobStates.finished)
      WorkflowOps.updateWorkflow(workflow.id)(shirtJob.id)(JobStates.finished)

      WorkflowOps.discoverNext(workflow.id)(socksJob.id) must beRight {
        (jobNodes: Vector[Job]) ⇒
          jobNodes.size must be_==(0)
      }

      WorkflowOps.discoverNext(workflow.id)(undershortsJob.id) must beRight {
        (jobNodes: Vector[Job]) ⇒
          jobNodes.size must be_==(1)
          jobNodes must contain(be_==(pantsJob))
      }

      WorkflowOps.discoverNext(workflow.id)(shirtJob.id) must beRight {
        (jobNodes: Vector[Job]) ⇒
          jobNodes.size must be_==(1)
          jobNodes must contain(be_==(tieJob))
      }

      WorkflowOps.updateWorkflow(workflow.id)(tieJob.id)(JobStates.finished)
      WorkflowOps.updateWorkflow(workflow.id)(pantsJob.id)(JobStates.finished)

      WorkflowOps.discoverNext(workflow.id)(pantsJob.id) must beRight {
        (jobNodes: Vector[Job]) ⇒
          jobNodes.size must be_==(2)
          jobNodes must contain(be_==(shoesJob), be_==(beltJob))
      }

      WorkflowOps.updateWorkflow(workflow.id)(shoesJob.id)(JobStates.finished)

      WorkflowOps.discoverNext(workflow.id)(shoesJob.id) must beRight {
        (jobNodes: Vector[Job]) ⇒
          jobNodes.size must be_==(0)
      }

      WorkflowOps.getWorkflowStatus(workflow.id) must beSome
      WorkflowOps.getWorkflowStatus(java.util.UUID.randomUUID) must beNone

      // Reset it for the next subsequent test run
      WorkflowOps.updateWorkflow(workflow.id)(shirtJob.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(tieJob.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(jacketJob.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(beltJob.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(socksJob.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(shoesJob.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(pantsJob.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(beltJob.id)(JobStates.inactive)
      WorkflowOps.updateWorkflow(workflow.id)(undershortsJob.id)(JobStates.inactive)

      ok
    }
  }
}
*/

