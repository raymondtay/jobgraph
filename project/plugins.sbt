// The following plugins are in use
// [Scala style] http://www.scalastyle.org/sbt.html 
// [Scala Code coverage] https://github.com/scoverage/sbt-scoverage
// [Scala Code coverage to coveralls] https://github.com/scoverage/sbt-coveralls
// [Scala Distributable packager] https://github.com/xerial/sbt-pack
// [Scala simple documentation tool] http://tpolecat.github.io/tut/
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.2.5")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.9.3")
addSbtPlugin("org.tpolecat" % "tut-plugin" % "0.6.4")

