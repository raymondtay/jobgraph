package hicoden.jobgraph.configuration.step

import hicoden.jobgraph.configuration.step.model._

/**
  * Behaviors related to loading the configuration into the Workflow Engine
  * @author Raymond Tay
  * @version 1.0
  */
trait Loader {
  import cats._, data._, implicits._

  /**
    * Hydrates the state (i.e. given job descriptor table) with the
    * job configurations. Note: If the key doesn't exist, then the (key,value)
    * pair is added else the value replaces the key.
    * @param job configs
    * @param current job descriptor table
    * @return updated job descriptor table with the job configs
    */
  def hydrateJobConfigs = Reader { (cfgs: List[JobConfig]) ⇒
    for {
      s ← State.get[JobDescriptorTable]
      _ ← State.modify{(table: JobDescriptorTable) ⇒
                  var mutT = collection.mutable.HashMap(table.toList: _*)
                  cfgs.collect{ case cfg ⇒ (cfg.id, cfg) }.foreach( pair ⇒ mutT.update(pair._1, pair._2) )
                  collection.immutable.HashMap(mutT.toList: _*)
                }
      s2 ← State.get[JobDescriptorTable]
    } yield s2
  }

}
