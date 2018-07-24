package hicoden.jobgraph.engine.persistence

import hicoden.jobgraph.configuration.engine.{Parser => EngineParser}

object Transactors extends EngineParser {

  import doobie._, doobie.implicits._
  import cats._, cats.data.Validated._, cats.effect._, cats.implicits._

  // A RTE is thrown, when the database is not accessible
  lazy val xa = 
    loadEngineDb("jobgraph.db").map(cfg ⇒ Transactor.fromDriverManager[IO](cfg.driver, cfg.name, cfg.username, cfg.password)) match {
      case Valid(xaObj) ⇒ xaObj
      case Invalid(_) ⇒ throw new RuntimeException("The postgresql database in persistence.conf is not accessible")
    }

  lazy val y = xa.yolo

}
