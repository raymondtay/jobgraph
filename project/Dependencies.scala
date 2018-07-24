import sbt._

object Dependencies {

  val scioVersion        = "0.5.2" // Apache Beam Scala library from Spotify
  val beamVersion        = "2.4.0" // Apache Beam 
  val akkaVersion        = "2.5.12" // Akka Actor library from Lightbend
  val catsVersion        = "1.0.1"  // FP library from Typelevel
  val scalaCheckVersion  = "1.13.4" // Property testing library
  val specs2Version      = "4.0.1"  // Scala library for unit tests
  val scalatestVersion   = "3.0.5" // Scala library for unit tests
  val quiverVersion      = "7.0.19" // Scala library for multigraph
  val typesafeCfgVersion = "1.3.2"  // Scala library for configuraiton
  val akkaHttpVersion    = "10.1.1" // Akka http library
  val doobieVersion      = "0.5.3"
  val logbackClassicVersion = "1.2.3" // logging library
  val scalaLoggingVersion   = "3.9.0" // logging library
  val pureConfigVersion     = "0.9.1" // Scala library for removing boilerplate from reading configuration files
  val fastparseVersion      = "1.0.0" // Scala library for parsing ASTs, DSLs etc
  val circeVersion          = "0.9.3" // Scala library for processing JSON

  val postgresqlLibs = Seq(
    // Start with this one
    "org.tpolecat" %% "doobie-core"      % doobieVersion,
    // And add any of these as needed
    "org.tpolecat" %% "doobie-postgres"  % doobieVersion, // Postgres driver 42.2.2 + type mappings.
    "org.tpolecat" %% "doobie-specs2"    % doobieVersion // Specs2 support for typechecking statements.
  )

  val loggingLibs = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    "ch.qos.logback" % "logback-classic" % logbackClassicVersion
  )

  val typesafeConfigLib    = "com.typesafe" % "config"        % typesafeCfgVersion
  val quiverLib            = "io.verizon.quiver" %% "core"    % quiverVersion
  val catsLib              = "org.typelevel" %% "cats-core"   % catsVersion
  val scalaCheckTest       = "org.scalacheck" %% "scalacheck" % scalaCheckVersion
  val fastparse            = "com.lihaoyi" %% "fastparse"     % fastparseVersion
  val specs2Test           = "org.specs2" %% "specs2-core"       % specs2Version
  val scalatestLib         = "org.scalatest" %% "scalatest"      % scalatestVersion
  val specs2ScalaCheckTest = "org.specs2" %% "specs2-scalacheck" % specs2Version
  val actorLib             = "com.typesafe.akka" %% "akka-actor"     % akkaVersion
  val actorStreamLib       = "com.typesafe.akka" %% "akka-stream"    % akkaVersion
  val actorTest            = "com.typesafe.akka" %% "akka-testkit"   % akkaVersion
  val pureConfig           = "com.github.pureconfig" %% "pureconfig" % pureConfigVersion
  val circeLibs            = Seq( "io.circe" %% "circe-core", "io.circe" %% "circe-generic", "io.circe" %% "circe-parser", "io.circe" %% "circe-optics").map(_ % circeVersion)
  val akkaHttpLib          = "com.typesafe.akka" %% "akka-http" % akkaHttpVersion

  val scioLibs = Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
  )

  val scioTestLib = "com.spotify" %% "scio-test" % scioVersion
  val akkaHttpTestLib = "com.typesafe.akka" %% "akka-http-testkit" % "10.1.1"

  val generalLibs = actorLib :: actorStreamLib :: catsLib ::
                    quiverLib :: typesafeConfigLib :: pureConfig ::
                    fastparse :: akkaHttpLib :: Nil ++ loggingLibs ++ scioLibs ++ circeLibs ++ postgresqlLibs

  val testLibs = Seq(specs2Test , specs2ScalaCheckTest, actorTest, scioTestLib, scalatestLib, akkaHttpTestLib).map(_ % Test) ++ circeLibs
}
