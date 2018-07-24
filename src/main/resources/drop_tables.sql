-- Executing of this script means it will drop everything
-- except the user. See [[db.sql]] for more details.
\c jobgraph
ALTER TABLE workflow_template OWNER TO postgres;
ALTER TABLE job_template OWNER TO postgres;

DROP TABLE IF EXISTS job_template;
DROP TABLE IF EXISTS workflow_template;

-- According to postgresql docs, we cannot drop a currently or "open" 
-- database; the solution is to enter as the superuser e.g. postgres and
-- remove it. Uncommenting the following does not work; but leaving it here
-- might be a good idea.
-- <<< DONT UNCOMMENT >>> DROP DATABASE IF EXISTS jobgraph;

