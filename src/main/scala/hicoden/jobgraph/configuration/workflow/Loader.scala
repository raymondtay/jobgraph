package hicoden.jobgraph.configuration.workflow

import hicoden.jobgraph.configuration.workflow.model._

/**
  * Behaviors related to loading the (i.e. validated) workflow configuration(s) into the Workflow Engine
  * @author Raymond Tay
  * @version 1.0
  */
trait Loader {
  import cats._, data._, implicits._

  /**
    * Hydrates the state (i.e. given job descriptor table) with the
    * workflow configurations. Note: If the key doesn't exist, then the (key,value)
    * pair is added else the value replaces the key.
    * @param job configs
    * @param current job descriptor table
    * @return updated job descriptor table with the job configs
    */
  def hydrateWorkflowConfigs = Reader { (cfgs: List[WorkflowConfig]) ⇒
    for {
      s ← State.get[WorkflowDescriptorTable]
      _ ← State.modify{(table: WorkflowDescriptorTable) ⇒
                  var mutT = collection.mutable.HashMap(table.toList: _*)
                  cfgs.collect{ case cfg ⇒ (cfg.id, cfg) }.foreach( pair ⇒ mutT.update(pair._1, pair._2) )
                  collection.immutable.HashMap(mutT.toList: _*)
                }
      s2 ← State.get[WorkflowDescriptorTable]
    } yield s2
  }

}
