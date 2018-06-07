# jobgraph

# What it does

This is an engine that allows you to do two things:
- Define `job`(s) in the system and create `workflow`(s) using that and submits
  it to a Apache Beam framework for execution
- To do all that, you have to write jobs using Apache Beam's programming model
- Craft a processing graph which details your execution dependency and submit
  that to `jobgraph` and it takes care of executing the job for you.


## What do i need to know about JobGraph

Basic idea is to provide a mechanism that allows the user to define:
- What a _step_ is
  - That usually means a format has to be defined
  - A step is, for now, a `Apache Beam` job (i.e. runnable) and `jobgraph` is responsible for starting the job.
- How to describe a _workflow_ by stringing 1 or more _steps_ 
  - A configuration file (e.g. see [[workflows.conf]]) would describe how the computation would proceeed by definining a _job graph_. Take note that the steps defined in _ANY_ workflow must already exist in the system.
- The _job graph_ defines the node(s) where the system will wait. In other
  words, the system is designed to be asynchronous by default unless otherwise.
- A few common operations to be provided 

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


