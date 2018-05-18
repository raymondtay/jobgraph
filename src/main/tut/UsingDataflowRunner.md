Here is an example of how to use the Dataflow Runner and the scenario here
is that the jobid has been returned to the engine (via a callback called from
the executing job) and we use the `DataflowRunner` to trigger the monitoring.

The script that is invoked contains the commands and expected environment
variable, `JOB_ID` to be present when executing. The entire output is captured
and parsed appropriately.

The parsed JSON payload is housed in the field name "returns" of the returned
`Context` object which you can lift out for further processing.

```tut

import hicoden.jobgraph.fsm._, runners._
class Demo {
  val ctx =
    Context(
      getClass.getClassLoader.getResource("fake_gcloud_monitor_job.sh").getPath.toString :: Nil, "2018-05-16_22_09_13-15128694865182730787",
      io.circe.Json.Null)
}
val demo = new Demo
val r = new DataflowRunner
r.run(demo.ctx)(jsonParser.parse)

```


