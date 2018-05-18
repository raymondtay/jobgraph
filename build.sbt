import Dependencies._

// Settings
val commonSettings = Seq(
  name := "Job Engine",
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
    commonSettings ++ codeCoverageSettings,
    libraryDependencies ++= (generalLibs ++ testLibs)
  ).enablePlugins(PackPlugin, TutPlugin)

// Excluded the "examples" from the coverage
coverageExcludedPackages := "hicoden\\.jobgraph\\.CLRSBumsteadGraph;hicoden\\.jobgraph\\.ConvergeGraph;hicoden\\.jobgraph\\.ScatterGatherGraph;hicoden\\.jobgraph\\.examples\\.*"

// Need for the kind project so that i dont have to use type-lambdas
addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4")

// Needed for Spotify's scio
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

// All tests are run sequentially
parallelExecution in Test := false

packMain := Map("Job Engine" -> "hicoden.jobgraph.engine.Engine")

unmanagedResourceDirectories in Compile += baseDirectory.value / "scripts"
unmanagedResourceDirectories in Test += baseDirectory.value / "src" / "test" / "scripts"
unmanagedResourceDirectories in Tut += baseDirectory.value / "tut-scripts"

