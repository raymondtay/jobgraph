-- Executing of this script means it will drop everything
-- except the user. See [[db.sql]] for more details.
\c jobgraph
ALTER TABLE workflow_template OWNER TO postgres;
ALTER TABLE job_template      OWNER TO postgres;
ALTER TABLE job_rt            OWNER TO postgres;
ALTER TABLE workflow_rt       OWNER TO postgres;

DROP TABLE IF EXISTS workflow_rt       CASCADE;
DROP TABLE IF EXISTS job_rt            CASCADE;
DROP TABLE IF EXISTS job_template      CASCADE;
DROP TABLE IF EXISTS workflow_template CASCADE;

DROP TYPE IF EXISTS Runner         CASCADE;
DROP TYPE IF EXISTS WorkflowStates CASCADE;
DROP TYPE IF EXISTS JobStates      CASCADE;
DROP TYPE IF EXISTS JobConfigRT    CASCADE;

-- According to postgresql docs, we cannot drop a currently or "open" 
-- database; the solution is to enter as the superuser e.g. postgres and
-- remove it. Uncommenting the following does not work; but leaving it here
-- might be a good idea.
-- <<< DONT UNCOMMENT >>> DROP DATABASE IF EXISTS jobgraph;

