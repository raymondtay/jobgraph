package hicoden.jobgraph

// This graph is from the CLRS Algorithm textbook page 613 where Professor
// Bumstead needs to figure how what he needs to do (in sequence) so that he
// can get his day started; run the [[Engine]]
object CLRSBumsteadGraph {
  
  import quiver._

  val shirtJob = Job("shirt")
  val tieJob = Job("tie")
  val jacketJob = Job("jacket")
  val beltJob = Job("belt")
  val pantsJob = Job("pants")
  val undershortsJob = Job("undershorts")
  val socksJob = Job("socks")
  val shoesJob = Job("shoes")
  val watchJob = Job("watch")

  val shirt  = LNode(shirtJob, shirtJob.id)
  val tie    = LNode(tieJob, tieJob.id)
  val jacket = LNode(jacketJob, jacketJob.id)
  val belt   = LNode(beltJob, beltJob.id)
  val pants  = LNode(pantsJob, pantsJob.id)
  val undershorts = LNode(undershortsJob, undershortsJob.id)
  val socks = LNode(socksJob, socksJob.id)
  val shoes = LNode(shoesJob, shoesJob.id)
  val watch = LNode(watchJob, watchJob.id)
  
  val e1  = LEdge(shirtJob,tieJob,"shirt->tie")
  val e2  = LEdge(shirtJob,beltJob, "shirt->belt")
  val e3  = LEdge(tieJob,jacketJob,"tie->jacket")
  val e4  = LEdge(beltJob,jacketJob,"belt->jacket")
  val e5  = LEdge(pantsJob,beltJob,"pants->belt")
  val e6  = LEdge(undershortsJob,pantsJob,"undershorts->pants")
  val e7  = LEdge(undershortsJob,shoesJob,"undershorts->shoes")
  val e8  = LEdge(pantsJob,shoesJob,"pants->shoes")
  val e9  = LEdge(socksJob,shoesJob,"socks->shoes")
  val e10 = LEdge(watchJob,watchJob,"watch->watch")
  
  import WorkflowOps._
  val workflow = createWf(Seq(shirt, tie, jacket, belt, pants, undershorts, socks, shoes, watch))(Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10))
}
// digraph [a -> b, a -> c, c -> d, b -> d]
object ScatterGatherGraph {

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

}

// digraph [a -> b, c -> b, d -> b]
object ConvergeGraph {

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

}
