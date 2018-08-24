The purpose of this README is to give the reader an overview of what kind of
files are in here and more importantly to let them what are they for, exactly.

# Database schema files

`Jobgraph` is reliant on a local database to manage its state for recovering
from an offline mode. What you see here are two files:

* drop_tables.sql
* createdb_tables.sql

No prizes for guessing what they do but in fact, the first script would deal
with all of the facets of creating a database whilst the second script listed
there deals with the creation of database tables.

The _creation_ process is duplicated in the Circle CI build file, found at the
root of this project for the purpose of supporting execution of database
related tests.

# Database configuration file

`Jobgraph` connects to a running PostgreSQL server (version 9.6.5) via
configuration and its abstracted away by reading the configuration file:

* persistence.conf

*Note:* In general, if you deployed a different database for `jobgraph` to use
then please ensure you give it the right permissions

# Jobgraph job configuration files

If you were to consult the documentation, you would noticed that a
specification of a job is defined largely in two parts:

* What a job _is_
* How to execute it

`Jobgraph` allows you to define as many configuration files as you possibly
want and eventually you have to simply refer all those jobs by _including_ them
into the primal configuration file we are using i.e. _application.conf_ then
`jobgraph` would load it and validate that all mandatory fields are present.

An example of the configuration file: 

* steps.conf
* steps-another.conf

*Note:* Take note that each job understands how many automatic restarts would
occur and how long a time to wait for a job to complete its work.

# Jobgraph workflow configuration files

As in the previous paragraph, `Jobgraph` also follows the convention that
workflows are created and defined in configuration files and subsequently
loaded by `Jobgraph` upon start up.

An example of the workflow configuration file:

* workflows.conf

# General configuration file

Now that you have read the previous paragraphs of how things can be configured
and here is how you can reference them into the main configuration file also
known otherwise as _application.conf_ 

If you think about it, its a lot like _composing functions_; and over here, it
is conventional to list all the properties used internally by `jobgraph`. You
will discover that there is only a few things here, briefly listed:

* mesos settings - which tells you where to locate the mesos clusters in which
                   to spread the work load.

* jobgraph settings - the communication channel in which remote jobs executing
                      in the cluster can notify `jobgraph` to enable automatic
                      job monitoring.


