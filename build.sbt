import org.scoverage.coveralls.Imports.CoverallsKeys._
import Dependencies._

// Settings
val commonSettings = Seq(
  name := "JobEngine",
  organization := "org.nugit",
  description := "Multigraph Job Modelling",
  scalaVersion := "2.12.4",
  scalacOptions ++= Seq("-deprecation", "-feature", "-Yrangepos", "-Ypartial-unification")
)

val codeCoverageSettings = Seq(
 coverageExcludedPackages := "",
 coverageMinimum := 80,
 coverageFailOnMinimum := true
)

// project settings
lazy val jobgraph = (project in file("."))
  .settings(
    version := "0.9",
    commonSettings ++ codeCoverageSettings,
    libraryDependencies ++= (generalLibs ++ testLibs)
  ).enablePlugins(PackPlugin, TutPlugin)

// Excluded the "examples" from the coverage
coverageExcludedPackages := "hicoden\\.jobgraph\\.engine\\.Engine;hicoden\\.jobgraph\\.CLRSBumsteadGraph;hicoden\\.jobgraph\\.ConvergeGraph;hicoden\\.jobgraph\\.ScatterGatherGraph;hicoden\\.jobgraph\\.examples\\.*"

// Need for the kind project so that i dont have to use type-lambdas
addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4")

// Needed for Spotify's scio
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

// All tests are run sequentially
parallelExecution in Test := false

// sbt-pack mandates that all settings are mapped by the name of the key
packMain           := Map("JobEngine" -> "hicoden.jobgraph.engine.Engine")
packExtraClasspath := Map("JobEngine" -> Seq("${PROG_HOME}/dataflow-scripts"))
packJvmOpts        := Map("JobEngine" -> Seq("-Xms4G",
                                             "-Xmx4G",
                                             "-XX:+UseParNewGC",
                                             "-XX:+UseConcMarkSweepGC",
                                             "-XX:+CMSParallelRemarkEnabled",
                                             "-XX:SurvivorRatio=8",
                                             "-XX:MaxTenuringThreshold=1",
                                             "-XX:CMSInitiatingOccupancyFraction=75",
                                             "-XX:+UseCMSInitiatingOccupancyOnly",
                                             "-XX:CMSWaitDuration=10000",
                                             "-XX:+CMSParallelInitialMarkEnabled",
                                             "-XX:+CMSEdenChunksRecordAlways"))

packResourceDir    += (baseDirectory.value / "scripts" -> "dataflow-scripts")

unmanagedClasspath           in Runtime += baseDirectory.value / "dataflow-scripts"
unmanagedResourceDirectories in Compile += baseDirectory.value / "scripts"
unmanagedResourceDirectories in Test += baseDirectory.value / "src" / "test" / "scripts"
unmanagedResourceDirectories in Tut += baseDirectory.value / "tut-scripts"

coverallsToken := Some("6FbkmXztn0ntFMHTa0Gj7iu2BeA4elYkD")

