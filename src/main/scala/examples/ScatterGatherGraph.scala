package hicoden.jobgraph.examples

import akka.actor._
import hicoden.jobgraph.engine._

// digraph [a -> b,
//          a -> c,
//          c -> d,
//          b -> d]
object ScatterGatherGraph extends App {
  import hicoden.jobgraph._
  import quiver._

  val jobA = Job("job-a")
  val jobB = Job("job-b")
  val jobC = Job("job-c")
  val jobD = Job("job-d")

  val node1 = LNode(jobA, jobA.id)
  val node2 = LNode(jobB, jobB.id)
  val node3 = LNode(jobC, jobC.id)
  val node4 = LNode(jobD, jobD.id)

  val e1 = LEdge(jobA, jobB, "a -> b")
  val e2 = LEdge(jobA, jobC, "a -> c")
  val e3 = LEdge(jobC, jobD, "c -> d")
  val e4 = LEdge(jobB, jobD, "b -> d")

  import WorkflowOps._

  val workflow = createWf(collection.immutable.Seq(node1, node2, node3, node4))(collection.immutable.Seq(e1, e2, e3, e4))

  val actorSystem = ActorSystem("EngineSystem")
  val engine = actorSystem.actorOf(Props(classOf[Engine]), "Engine")

  // start a job graph running
  engine ! StartWorkflow(workflow)

  Thread.sleep(8000)

  engine ! StopWorkflow(workflow.id)

  Thread.sleep(4000)

  // Stop the engine
  actorSystem.terminate()
}

// digraph [a -> b,
//          c -> b,
//          d -> b]
object ConvergeGraph extends App {
  import hicoden.jobgraph._
  import quiver._

  val jobA = Job("job-a")
  val jobB = Job("job-b")
  val jobC = Job("job-c")
  val jobD = Job("job-d")

  val node1 = LNode(jobA, jobA.id)
  val node2 = LNode(jobB, jobB.id)
  val node3 = LNode(jobC, jobC.id)
  val node4 = LNode(jobD, jobD.id)

  val e1 = LEdge(jobA, jobB, "a -> b")
  val e2 = LEdge(jobC, jobB, "c -> b")
  val e3 = LEdge(jobD, jobB, "d -> b")

  import WorkflowOps._

  val workflow = createWf(collection.immutable.Seq(node1, node2, node3, node4))(collection.immutable.Seq(e1, e2, e3))

  val actorSystem = ActorSystem("EngineSystem")
  val engine = actorSystem.actorOf(Props(classOf[Engine]), "Engine")

  // start a job graph running
  engine ! StartWorkflow(workflow)

  Thread.sleep(8000)

  engine ! StopWorkflow(workflow.id)

  Thread.sleep(4000)

  // Stop the engine
  actorSystem.terminate()
}
