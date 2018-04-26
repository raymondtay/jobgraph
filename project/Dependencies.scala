import sbt._

object Dependencies {

  val scioVersion        = "0.5.2" // Apache Beam Scala library from Spotify
  val beamVersion        = "2.4.0" // Apache Beam 
  val akkaVersion        = "2.5.12" // Akka Actor library from Lightbend
  val catsVersion        = "1.0.1"  // FP library from Typelevel
  val scalaCheckVersion  = "1.13.4" // Property testing library
  val specs2Version      = "4.0.1"  // Scala library for unit tests
  val quiverVersion      = "7.0.19" // Scala library for multigraph
  val typesafeCfgVersion = "1.3.2"  // Scala library for configuraiton
  val logbackClassicVersion = "1.2.3" // logging library
  val scalaLoggingVersion   = "3.9.0" // logging library

  val loggingLibs = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    "ch.qos.logback" % "logback-classic" % logbackClassicVersion
  )

  val typesafeConfigLib    = "com.typesafe" % "config" % typesafeCfgVersion
  val quiverLib            = "io.verizon.quiver" %% "core" % quiverVersion
  val catsLib              = "org.typelevel" %% "cats-core" % catsVersion
  val scalaCheckTest       = "org.scalacheck" %% "scalacheck" % scalaCheckVersion
  val specs2Test           = "org.specs2" %% "specs2-core" % specs2Version
  val specs2ScalaCheckTest = "org.specs2" %% "specs2-scalacheck" % specs2Version
  val actorLib             = "com.typesafe.akka" %% "akka-actor" % akkaVersion
  val actorTest            = "com.typesafe.akka" %% "akka-testkit" % akkaVersion

  val scioLibs = Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    // optional dataflow runner
    // "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
  )

  val scioTestLib = "com.spotify" %% "scio-test" % scioVersion

  val generalLibs = actorLib :: catsLib :: quiverLib :: typesafeConfigLib :: Nil ++ loggingLibs ++ scioLibs
  val testLibs = Seq(specs2Test , specs2ScalaCheckTest, actorTest, scioTestLib).map(_ % Test)
}
