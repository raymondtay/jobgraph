package hicoden.jobgraph.engine.persistence

import org.specs2.{ScalaCheck, Specification}
import org.specs2._

import doobie._
import doobie.implicits._
import cats._
import cats.data._
import cats.effect.IO
import cats.implicits._

import hicoden.jobgraph.configuration.step.model._


// Note: As in Doobie 0.5.x, Fragments in Doobie are opaque objects therefore
// it limits the extent of the tests.
class FragmentFunctionsSpecs extends Specification with ScalaCheck with FragmentFunctions { def is = sequential ^ s2"""
  When a custom type is passed with an empty container, 'arrayExpr' should generate a empty Fragment (not null)    $emptyFragmentWhenEmpty
  When a custom type is passed an non-empty container, 'arrayExpr' should generate a non-empty Fragment (not null) $nonEmptyFragmentWhenEmpty
  When a custom type is passed an Runner, 'runnerExpr' should generate a non-empty Fragment (not null)       $nonEmptyFragmentWhenRunnerValid
  """
  def emptyFragmentWhenEmpty = {
    arrayExpr.run(Nil) must not beNull
  }
  def nonEmptyFragmentWhenEmpty = {
    arrayExpr.run(List("1","2")) must not beNull
  }
  def nonEmptyFragmentWhenRunnerValid = {
    val runner = Runner(module = "module.m", runner = "/path/to/execfile", Nil)
    runnerExpr.run(runner) must not beNull
  }
}
