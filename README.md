# Job Graph

The problem this engine solves was to apply transformation logic to unbounded datasets.
The unbounded dataset problem had essentially two faces:

* The size of the dataset was undeterministic (i.e. data arrives like gushing out of a _hose_ or trickling through a _tap_ at a periodic rate etc)
* You never know quite when the datum would arrive (Most frameworks had little or no support for handling _out of order_ events, late arriving events etc)

The Dataflow Model paper by Google first laid the major ideas on how such a framework would work to solve this problem; the TLDR version of this story
is that Google contributed to Apache Beam which is lifted into a _cloud-native_ approach and came to be known as Google Dataflow. This was great because the Apache Beam implementation allow data processing functions to be defined and developers can create data pipelines by _chaining_ these functions which essentially creates a graph processing topology; for the computer science geeks amongst you, you would know it as a DAG.

I thought Apache Beam was great! but it didn't really solve my problem for two (2) reasons:

* The Apache Beam functions only allow ETLs to be defined using _homogeneous_ approach i.e. Apache Beam Python ETL or Java ETLs; _mixins_ were not supported
* The [Google Dataflow](https://cloud.google.com/dataflow) was a SaaS solution and i needed the solution to be delivered via a cloud-native approach via IaC

What was the need? I needed the mixin-approach for a simple reason is that we were migrating a large number of already built data pipelines and with a little effort, we can de-compose these ETLs and re-integrate them into _jobgraph_; the second need was simply because i did not wish to be 100% reliant on Google's Dataflow and there were other additional niceties we can built into the architecture (one of these niceties was to execute the pipelines locally in the private DC).

Hence, the need for _**JobGraph**_ ☺

![Cats Friendly Badge](https://typelevel.org/cats/img/cats-badge-tiny.png)

# Job Graph Internals

There is no restriction on the nature of what a JobGraph job does, you can create anything you like (i.e. Python or Java programs, Apache Beam processing functions) and have their binaries readily available via Java or Python's mechanism. When you are ready, you can _chain_ these jobs into pipelines for unit-testing / integration-testing or production delivery.

The execution choices of JobGraph pipelines is :

* (a) Same host as `JobGraph` i.e. _local-mode_ (*Note:* Not recommended)
* (b) Delegate to Google Dataflow infrastructure
* (c) Delegate to any of the nodes available in the compute cluster

Option (b) is favored over Option (c) when you have a huge job that would be
executed in Google's infrastructure (for example: you have a sizable monetary
budget); otherwise you can exercise Option(c) by switching the type of runners.

To enable Option(b), which is the default, you have to do the following:
- Make sure at least one (i.e. 1) Apache Mesos cluster is alive and configure
  it in the `application.conf`.
- `MesosDataflow:java` and `--runner=DataflowRunner` for Java jobs
- `MesosDataflow:python` and `--runner DataflowRunner` for Python jobs

To enable Option(c), which is not the default, you have to do the following:
- `Dataflow:java` and `--runner=DirectRunner` for Java jobs
- `Dataflow:python` and `--runner DirectRunner` for Python jobs

# Architecture
## Multi-tenant shared-compute Architecture
What you see here is, potentially but fictitiously, 2 customers having their
own jobs and workflows loaded and having their workload launched against any of
the nodes in the 2 (shown) compute clusters (powered by Apache Mesos);
asynchronously and concurrent.

*Note:* Neither of the tenants (i.e. customers) know the existent of the other
and they shouldn't know. This allows us, Nugit, to be grow/shrink/utilize all
compute capacity of the computational power with a workload that is distributed throughout.

A secondary advantage is to accelerate Machine Learning frameworks like Apache
Spark, Google TensorFlow to deliver faster business results to the customers.

![Proposed Production Architecture](./imgs/multi-tenant-shared-compute.png)
## Single-tenant shared-compute Architecture

What you see here is a proof-of-concept where a single customer's workflows
will run asynchronously (concurrently as well) and simultaneously across 2 compute clusters (thereby
spreading the work load).

## Sandbox in the Google Cloud
![Sandbox Architecture](./imgs/single-tenant-dedicated-compute.png)

## What do i need to know about JobGraph

Basic idea is to provide a mechanism that allows the user to define:
- What a _step_ (aka Job) is
  - That usually means a format has to be defined
  - A step is, for now, a `Apache Beam` job (i.e. runnable) and `jobgraph` is responsible for starting the job.
- How to describe a _workflow_ by chaining 1 or more _steps_ 
  - A configuration file (e.g. see [[workflows.conf]]) would describe how the computation would proceeed by definining a _job graph_. Take note that the steps defined in _ANY_ workflow must already exist in the system.
- The _job graph_ defines the node(s) where the system will wait. In other
  words, the system is designed to be asynchronous by default unless otherwise.

## What's a Job

A _step_ is essentially a computer program that will be executed, given a group
of parameters (e.g. inputs, outputs) and executes under the context of a
requester (e.g. `user-id`, `oauth`) on its behest.

A step is defined by the following (non-exhaustively):
- Name
- Description
- Inputs (perhaps type of inputs)
- Working Directory (this is some storage that is made available e.g. S3, GCP, multi-tiered storage systems like Alluxio)
- Session Identifier (this is a unique identifier in the entire system which identifies it)
- Runner (describes how to get the program executed e.g. `Apache Beam`)
- Run-As (the identity of the requester)

## What's a Workflow

A _workflow_ is essentially a graph of computations (i.e. graph of _steps_) to
be carried out.

A workflow is defined by the following (non-exhaustively):
- Name
- Id
- Description
- Steps (a list of names which point to the steps currently in the system)
- Job graph (a mechanism to describe how to execute the string of actions)

## What can we do with JobGraph

If you are the developer of `JobGraph` engine, you perform the following
programmtically or via ReST calls.

If you are the user of `JobGraph`, you can perform the following only via ReST
calls.

In either case, please refer to the confluence page [here](https://nugitco.atlassian.net/wiki/spaces/ND/pages/525303812/JobGraph+ReST+Interfaces)
for more details.

- Create a workflow
- Start a workflow
- Query a workflow (aka _monitoring workflow_)
- List all workflows
- Stop a workflow
- Update a workflow
- Create a job
- List all job(s)
- Monitor a Job (job ∈ Workflow)

## Limitations of JobGraph

- The workflows and jobs configuration does not survive a reboot
- If your job is not a Apache Beam job, then `JobGraph` cannot automatically
  monitor and manage the lifecycle of your workflow for you.
- Workflows cannot be restarted
- Workflows cannot be scheduled
- ~~Job parameters cannot be altered after you have created it; you need to
  create another job~~
  - Work completed in [HCD-63](https://nugitco.atlassian.net/browse/HCD-63)
- 

We will be plugging these soon.

# References
- See the `install_postgresql.md` on instructions to install postgresql database version 9.6
