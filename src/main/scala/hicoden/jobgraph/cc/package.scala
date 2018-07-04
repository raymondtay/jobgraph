package hicoden.jobgraph

// 'cc' a.k.a compute cluster
package object cc {

  import cats._, data._, implicits._
  import io.circe._, io.circe.syntax._
  import io.circe.optics.JsonPath._

  type FrameworkId = java.util.UUID

  // These states are registered in the [[mesos.proto]] file of Apache Mesos
  // 1.6.0.
  object FrameworkStates extends Enumeration {
    type FrameworkState = Value
    val TASK_STAGING, TASK_RUNNING, TASK_STARTING, TASK_KILLING, TASK_FINISHED, TASK_FAILED, TASK_KILLED, TASK_ERROR, TASK_LOST, TASK_DROPPED, TASK_UNREACHABLE, TASK_GONE, TASK_GONE_BY_OPERATOR, TASK_UNKNOWN = Value
  }

  // Pure functions that examine the metric data in the JSON structure 
  // and looks for tasks with different states.
  def ccStatsToClusterMetrics : Reader[Json, List[ClusterMetrics]] = Reader{ (j: Json) ⇒
    val frameworkId    = root.tasks.each.id.string
    val frameworkState = root.tasks.each.state.string
    frameworkId.getAll(j).zip(frameworkState.getAll(j)).map{
      p ⇒ ClusterMetrics(java.util.UUID.fromString(p._1), FrameworkStates.withName(p._2))
    }
  }

  def getRunningTasks = Reader{ (j: Json) ⇒
    root.tasks.each.filter(root.state.string.exist(_ == FrameworkStates.TASK_RUNNING.toString)).id.string.getAll(j)
  }
  def getStagingTasks = Reader{ (j: Json) ⇒
    root.tasks.each.filter(root.state.string.exist(_ == FrameworkStates.TASK_STAGING.toString)).id.string.getAll(j)
  }
  def getStartingTasks = Reader{ (j: Json) ⇒
    root.tasks.each.filter(root.state.string.exist(_ == FrameworkStates.TASK_STARTING.toString)).id.string.getAll(j)
  }

}


