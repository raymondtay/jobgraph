-- User, once created, is not dropped.
CREATE USER jobgraphadmin with password 'password';

DROP DATABASE IF EXISTS jobgraph;
CREATE DATABASE jobgraph OWNER jobgraphadmin;

-- henceforth, user is 'postgres'
\c jobgraph

-- Workflow template
-- stores the configuration of the workflow
-- see [[WorkflowConfig]] for the in-memory model
--
-- Datafields description
-- **********************
-- 1/ `id` - There would not be more than 2^32 workflows in the system
-- 2/ `name` - name of workflow does not exceed 256
-- 3/ `description` - description can be potentially long and also empty
-- 4/ `jobgraph` - interpreted by engine as a DAG but it is a container of strings
--

CREATE TABLE IF NOT EXISTS workflow_template (
    id integer PRIMARY KEY,
    name varchar(256) NOT NULL,
    description text NULL,
    jobgraph text[] NOT NULL
);

-- 
-- Represents the configuration as associated with a particular job
-- see [[Runner]] for the in-memory model
CREATE TYPE Runner AS (
  module text,
  runner text,
  cliargs text[]
);


-- Job template
-- stores the configuration of the Job
-- see [[JobConfig]] for the in-memory model
--
-- Datafields description
-- **********************
-- 1/ `id` - There would not be more than 2^32 jobs in the system
-- 2/ `name` - name of job does not exceed 256
-- 3/ `description` - description can be potentially long and also empty
-- 4/ `sessionid` - a session identifier
-- 5/ `restart` - how many times to automatically restart, upon failure.
-- 6/ `runner` - jobgraph will interpret this datatype and construct the
-- necessary executable
--
CREATE TABLE IF NOT EXISTS job_template (
  id integer PRIMARY KEY,
  name varchar(256) NOT NULL,
  description text NULL,
  sessionid text NULL,
  restart integer NOT NULL,
  runner Runner NOT NULL
);

-- see [[WorkflowStates]] for the in-memory model mapping
CREATE TYPE WorkflowStates as ENUM(
  'started',
  'not_started',
  'done'
);

-- see [[JobStates]] for the in-memory model mapping
CREATE TYPE JobStates as ENUM(
  'inactive',
  'start',
  'active',
  'forced_termination',
  'finished'
);

CREATE TYPE JobConfigRT as (
  name text,
  description text,
  sessionid text,
  restart integer,
  runner Runner
);

-- The job_rt and workflow_rt represents the runtime representation of the jobs
-- and workflows
CREATE TABLE IF NOT EXISTS job_rt (
  id UUID PRIMARY KEY,
  job_template_id integer NOT NULL references job_template(id),
  config JobConfigRT NOT NULL,
  status JobStates NOT NULL
);

-- Waiting for the '[] ELEMENT REFERENCES <table>' to be realized in PSQL 10.+
CREATE TABLE IF NOT EXISTS workflow_rt (
  id serial PRIMARY KEY,
  wf_id UUID NOT NULL,
  wf_template_id integer NOT NULL references workflow_template(id),
  status WorkflowStates NOT NULL,
  job_id UUID[] NOT NULL
);

-- bring back the control to jobgraphadmin
ALTER TABLE workflow_template OWNER TO jobgraphadmin;
ALTER TABLE job_template OWNER TO jobgraphadmin;
ALTER TABLE job_rt OWNER TO jobgraphadmin;
ALTER TABLE workflow_rt OWNER TO jobgraphadmin;


