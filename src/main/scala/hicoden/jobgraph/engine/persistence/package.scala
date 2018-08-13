package hicoden.jobgraph.engine

import hicoden.jobgraph.JobId
//
// Read the README markdown file first.
// @author Raymond Tay
// @date 1.0
//
package object persistence {

  import cats._
  import cats.data._
  import cats.effect.IO
  import cats.implicits._
  import doobie._
  import doobie.implicits._
  import doobie.postgres._
  import doobie.postgres.implicits._

  import hicoden.jobgraph.configuration.workflow.model._
  import hicoden.jobgraph.configuration.step.model._

  // TODO: watch doobie releases to reduce the boiler plate code.
  trait FragmentFunctions {

    def arrayUUIDExpr = Reader{ (xs: List[JobId]) ⇒ if (xs.isEmpty) fr"ARRAY[]::uuid[]" else fr"ARRAY[" ++ xs.map(e ⇒ fr"$e").intercalate(fr",") ++ fr"]::uuid[]" }
    def arrayExpr = Reader{ (xs: List[String]) ⇒ if (xs.isEmpty) fr"ARRAY[]::text[]"  else fr"ARRAY[" ++ xs.map(e ⇒ fr"$e").intercalate(fr",") ++ fr"]::text[]" }

    def runnerExpr = Reader{ (r: Runner) ⇒
      fr"ROW("++ fr"${r.module},"++ fr"${r.runner},"++ arrayExpr(r.cliargs) ++ fr")"
    }

    def jobConfigExpr = Reader{ (c: JobConfig) ⇒
      fr"ROW(" ++ fr"${c.name}," ++ fr"${c.description}," ++ fr"${c.sessionid}," ++ fr"${c.timeout}, " ++ fr"${c.restart.max}, " ++ runnerExpr(c.runner) ++ fr")"
    }
  }

}
