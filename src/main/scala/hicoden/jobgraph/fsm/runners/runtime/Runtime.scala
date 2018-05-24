package hicoden.jobgraph.fsm.runners.runtime


object Functions {
  import cats._, data._, implicits._
  import hicoden.jobgraph.configuration.step.model.{JobConfig, ExecType, RunnerType}
  import ExecType._, RunnerType._

  def isPythonModule = Reader{ (cfg: JobContext) ⇒ cfg.runner.runner.split(":")(1) equals ExecType.python.toString }
  def isJavaModule = Reader{ (cfg: JobContext) ⇒ cfg.runner.runner.split(":")(1) equals ExecType.java.toString }

  //
  // This function would throw a [[UnsupportedOperationException]] and its
  // intentional because of the fact that it should not have happened.
  //
  def buildCommand = Reader{ (cfg: JobContext) ⇒
    if (isPythonModule(cfg)) buildPythonCommand(cfg) else
    if (isJavaModule(cfg)) buildJavaCommand(cfg) else throw new UnsupportedOperationException(s"Jobgraph only supports the following exception types : ${ExecType.values.mkString(",")}")
  }

  def buildPythonCommand = Reader{ (cfg: JobContext) ⇒
    (s"python -m ${cfg.runner.module} ${cfg.runner.cliargs.mkString(" ")}", cfg.workdir, Map("PYTHONPATH" -> cfg.workdir))
  }

  def buildJavaCommand = Reader{ (cfg: JobContext) ⇒
    (s"${cfg.runner.module} ${cfg.runner.cliargs.mkString(" ")}", cfg.workdir, Map())
  }

}
