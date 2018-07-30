package hicoden.jobgraph.engine

/**
  * Represents the command line options being passed to the Engine
  * 1/ "initDb" ; when you indicate it to be 'yes' then jobgraph will
  *    attempt to insert the static configurations as written into the
  *    "application.conf" into the database tables
  *    [[workflow_template]] and [[job_template]]
  *
  * 2/ Please describe each option from this point onwards.
  *
  */
case class EngineOpts(initDb : Option[Boolean] = None)

object EngineCliOptsParser {
  import scopt._
  import cats._, cats.data.Kleisli, implicits._

  private implicit val zeroEngineOpts = Zero.zero(EngineOpts())
  private val parser = new scopt.OptionParser[EngineOpts]("jobgraph") {
    head("Jobgraph Engine", "version : 0.9")

    opt[String]("initDb").
      valueName("init the postgresql database").
      validate(x ⇒ if (x.trim.toUpperCase == "YES" || x.trim.toUpperCase == "NO") success else failure("Please pass a 'yes' or 'no', case insensitive, will do.")).
      action((x, c) ⇒ if (x.trim.toUpperCase == "YES") c.copy(initDb = Some(true)) else c)
  }
  def parseCommandlineArgs : Kleisli[Option, Seq[String], EngineOpts] = Kleisli{ (args: Seq[String]) ⇒
    parser.parse(args, EngineOpts())
  }
}
