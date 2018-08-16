# Installation of Postgresql server and client, creating the database schemas

The following instructions should work on _ANY_ Debian-like OSes.

*Note:* Make sure you have `sudo` access rights before proceeding.

* Install _postgresql_ server and client
  * ```sudo apt-get install -y postgresql postgresql-client```
  * If prompted for a password, do enter something like : `password`; you don't have to do it this way but it would be easier...
* Next, alter the password of the default `postgres` user
  * ```sudo -u postgres psql postgres```
  * ```\password postgres``` (*note:* make sure its something secure but in this case its `password`)
* At this point in time, you need to edit the file `/etc/postgresql/9.6/main/pg_hba.conf` and change the following lines
  * from `local all postgres peer`
  * to `local all postgres md5`
  * and `local all jobgraphadmin md5` (*note:* make sure this particular line appears before the `local all all peer` else you are going to get an authentication error)
* Save the file `/etc/postgresql/9.6/main/pg_hba.conf`
* Now, restart the postgresql service so that we can log in again
  * ```sudo service postgresql restart```
* Check the status 
  * ```sudo service postgresqql status```
  * The following output is seen 
```bash
hicoden@dataflow-testing-only:~$ sudo service postgresql status
● postgresql.service - PostgreSQL RDBMS
   Loaded: loaded (/lib/systemd/system/postgresql.service; enabled; vendor preset: enabled)
   Active: active (exited) since Fri 2018-08-03 06:43:36 UTC; 11s ago
  Process: 1106 ExecStart=/bin/true (code=exited, status=0/SUCCESS)
 Main PID: 1106 (code=exited, status=0/SUCCESS)

Aug 03 06:43:36 dataflow-testing-only systemd[1]: Starting PostgreSQL RDBMS...
Aug 03 06:43:36 dataflow-testing-only systemd[1]: Started PostgreSQL RDBMS.
```
* Now, we are ready to create the database `jobgraph`, database user `jobgraphadmin` and populate it with schemas
  * ```psql -f ./createdb_tables.sql -U postgres```
* Below is an example of the expected output ... might look a little different by the time you see this.
```bash
Password for user postgres:
CREATE ROLE
psql:./createdb_tables.sql:4: NOTICE:  database "jobgraph" does not exist, skipping
DROP DATABASE
CREATE DATABASE
You are now connected to database "jobgraph" as user "postgres".
CREATE TABLE
CREATE TYPE
CREATE TABLE
CREATE TYPE
CREATE TYPE
CREATE TYPE
CREATE TABLE
CREATE TABLE
ALTER TABLE
ALTER TABLE
ALTER TABLE
ALTER TABLE
```
