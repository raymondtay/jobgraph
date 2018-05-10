package hicoden.jobgraph.configuration.workflow.internal

import hicoden.jobgraph.{Job,JobId,WorkflowId}
import hicoden.jobgraph.configuration.workflow.model._
import hicoden.jobgraph.configuration.step.JobDescriptorTable

import quiver._
import cats._, free._, data._, implicits._

//
// [[Internal]] is the conventional name, it appears, to be bestowed to code
// that handles all the "dirty" work of doing most of the natural things.
//


/**
  * Validate whether we can create the Multi-Graph or DAG for a workflow
  * submitted to [[jobgraph]].
  * @author Raymond Tay
  * @version 1.0
  */
trait Concretizer {

  type Vertex = String
  case class Forward(src: Vertex, dest: Vertex)

  /**
    * Attempts to reify the jobgraph presented in the workflow config
    * @param jobDescriptorTable this should contain the pre-loaded jobs in the
    *        engine which would be validated against when parsing the workflow's
    *        jobgraph. See [[StepLoaderSpecs]] [[WorkflowLoaderSpecs]] on how to hydrate the step(s)
    *        and workflow(s) configurations.
    * @param workflow some workflow configuration
    * @return A [[Either.Left]] value whose payload indicates some parsing error or a
    *         [[Either.Right]] where the payload is the 3-tuple (t,u,v) where t =
    *         source node, u = destination node and v = edge between t and u.
    */
  def reify(jobDescriptorTable: JobDescriptorTable) : Reader[WorkflowConfig, Either[List[fastparse.core.Parsed[Forward,Char,String]], (List[LNode[Job,JobId]], List[LEdge[Job,String]])]] =
    Reader{ (workflow: WorkflowConfig) ⇒

      val parsedResult : Either[List[fastparse.core.Parsed[Forward,Char,String]],List[Forward]] = collectVertices(workflow)

      parsedResult.fold( (parsingErrors: List[fastparse.core.Parsed[Forward,Char,String]]) ⇒ parsingErrors.asLeft,
        relationships ⇒ {
          verifyVerticesWith(jobDescriptorTable)(relationships).fold(
            (error: String)         ⇒ (List.empty[LNode[Job,JobId]], List.empty[LEdge[Job,String]]),
            (jobIndexes: List[Int]) ⇒ reifyRelationships(jobIndexes)(jobDescriptorTable)(relationships)
          ).asRight
        }
      )
    }

  // Parses the vertices in the given workflow configuration, collects them as
  // a 2-partition
  def collectVertices : Reader[WorkflowConfig, Either[List[fastparse.core.Parsed[Forward,Char,String]], List[Forward]]] =
    Reader{ (workflow: WorkflowConfig) ⇒
      val jobGraph = workflow.jobgraph
      val (ok, failed) =
        jobGraph.map(subgraph ⇒ parseVertices(subgraph)).
        partition{ case fastparse.core.Parsed.Success(_, _) ⇒ true; case fastparse.core.Parsed.Failure(_, _, _) ⇒ false }

      if (! ok.isEmpty) {
        ok.collect{ case fastparse.core.Parsed.Success(edge, _) ⇒ edge }.asRight
      } else failed.asLeft
    }

  // Attempts to parse the tree/backward edges it notices and returns either a
  // [[Parsed.Success]] where the payload carries the graph/subgraph representation
  // or [[Parsed.Failure]] which pretty much says that there is a parsing error.
  def parseVertices : Reader[String, fastparse.core.Parsed[Forward,Char,String]] =
    Reader { (graph: String) ⇒
      import fastparse.all._
      val number   = P( CharIn('0' to '9').rep(min=1,max=10) )
      val treeEdge = P(" ".? ~ number.! ~ " ".? ~ "->" ~ " ".? ~ number.! ~ End).map(p ⇒ Forward(p._1, p._2))
      val backEdge = P(" ".? ~ number.! ~ " ".? ~ "<-" ~ " ".? ~ number.! ~ End).map(p ⇒ Forward(p._2, p._1))
      val edgeParser = P(treeEdge | backEdge)
      edgeParser.parse(graph)
    }

  def verifyVerticesWith(jobDescriptorTable : JobDescriptorTable) : Reader[List[Forward], Either[String,List[Int]]] =
    Reader { (edges: List[Forward]) ⇒
      val vertices = collection.mutable.Set.empty[String]
      def parse : String ⇒ Either[String, Int] = (s: String) ⇒
        scala.util.Try(s.toInt).map(Right(_)).getOrElse(Left(s"$s is not a number."))
      def contains : Either[String, Int] ⇒  Either[String, Option[Int]] = (e : Either[String, Int]) ⇒
        e.map(item ⇒ Right(if (jobDescriptorTable.contains(item)) Some(item) else None)).getOrElse(Left(s"$e not present in job descriptor table."))
      def convert : Either[String, Option[Int]] ⇒ Either[String, Int] = (e: Either[String, Option[Int]]) ⇒
        e.map(someJobIndex ⇒ someJobIndex.fold(Right(-1))(Right(_))).getOrElse(Left(s"$e not present in job descriptor table."))

      edges.foreach(e ⇒ { vertices += e.src; vertices += e.dest })
      val result = Yoneda(vertices.toList).map(parse).map(contains).map(convert).run.sequence
      result match {
        case Right(data) if (data.find(_ == -1).size == 0) ⇒ result
        case Right(data) if (data.find(_ == -1).size != 0) ⇒ Left(s"At least 1 job whose configuration cannot be discovered.")
        case Left(_) ⇒ result
      }
    }

  // Converts the relationship into [[quiver]]'s graph implementation of the
  // labelled vertex and labelled edge.
  def reifyRelationships(jobIndexes: List[Int])(jobDescriptorTable: JobDescriptorTable) : Reader[List[Forward], (List[LNode[Job,JobId]], List[LEdge[Job,String]])] =
    Reader{ (edges: List[Forward]) ⇒
      val jobNodes = collection.immutable.HashMap(jobIndexes.map(index ⇒ (index, Job(jobDescriptorTable(index).name))) : _*)
      val nodesT = for {
        (index, job) ← jobNodes
      } yield (index, LNode(job,job.id))
      val ee = edges.map(edge ⇒ LEdge( jobNodes(edge.src.toInt), jobNodes(edge.dest.toInt), s"${edge.src} -> ${edge.dest}" ))

      (nodesT.values.toList, ee)
    }

}

