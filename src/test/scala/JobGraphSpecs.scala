package hicoden.jobgraph

/** 
  * Factory object for the jobgraph
  */
object Graph extends App {

  import quiver._

  val jobA = Job("job-a")
  val jobB = Job("job-b")
  val jobC = Job("job-c")
  val jobD = Job("job-d")

  val node1 = LNode(jobA, "job-a")
  val node2 = LNode(jobB, "job-b")
  val node3 = LNode(jobC, "job-c")
  val node4 = LNode(jobD, "job-d")

  val e1 = LEdge(jobA, jobB, "a -> b")
  val e2 = LEdge(jobA, jobC, "a -> c")
  val e3 = LEdge(jobC, jobD, "c -> d")
  val e4 = LEdge(jobB, jobD, "b -> d")

  val jobGraph = mkGraph(Seq(node1, node2, node3, node4), Seq(e1, e2, e3, e4))

  assert(jobGraph.successors(jobA).size == 2)
}
