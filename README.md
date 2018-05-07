# jobgraph
A multigraph approach to modelling workflows since a workflow is essentially a
_job graph_.

# Goals

Basic idea is to provide a mechanism that allows the user to define:
- What a _step_ is
  - That usually means a format has to be defined
  - A step is, for now, a `Apache Beam` job (i.e. runnable) and `jobgraph` is responsible for starting the job.
- How to describe a _workflow_ by stringing 1 or more _steps_ 
  - A configuration file (e.g. see [[workflows.conf]]) would describe how the computation would proceeed by definining a _job graph_. Take note that the steps defined in _ANY_ workflow must already exist in the system.
- The _job graph_ defines the node(s) where the system will wait. In other
  words, the system is designed to be asynchronous by default unless otherwise.
- A few common operations to be provided 

# Step

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

# Workflow

A _workflow_ is essentially a graph of computations (i.e. graph of _steps_) to
be carried out.

A workflow is defined by the following (non-exhaustively):
- Name
- Id
- Description
- Steps (a list of names which point to the steps currently in the system)
- Job graph (a mechanism to describe how to execute the string of actions)

When _any_ step is to be executed, the _workflow engine_ (component not yet
built) is suppose to start the step (attempt to start).
