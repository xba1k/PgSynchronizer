# PgSynchronizer

PgSynchronizer is a database synchronization utility intended specifically for
PostgreSQL. The goal was to create a tool capable of incrementally synchronizing
entire schema in a scenario where access to the underlying OS could be limited,
such as database-as-a-service with certain cloud providers.

The tool was inspired by pg_comparator, but with the important condition to
perform the synchronization using only built-in database means, without any
additional extensions having to be installed since access to do so may not
be available. I have also experimented with a variety of techniques to optimize
for database and network performance, which is the reason for a variety of
unused pieces of code.

There was no particular reason to do it in C, as the most of the workload takes
places on the databases themselves, it should be simple enough to re-implement
it in Java or other language.

## Build

Project depends on libpq (PostgreSQL client library). Assuming your environment
is setup with PostgreSQL development files and gcc suite, simply run "make".


## Run

Small self-explanatory config file (sync.conf) is provided. After you edit it
to make adjustments for your environment, simply run as :

***./pgsynchronizer /your/path/sync.conf***

## Limitations

* The tool will synchronize the tables specified in the config file, it will not
enumerate the tables automatically. 
* Sequences are currently not synchronized.
* Primary Key and Foreign Key constraints won't cause issues as constraints
are disabled on target database during execution. Unique constraints on the other
hand will cause issues, and should be removed from the target tables.
* Composite Primary Keys are not supported, but shouldn't cause issues - tables
with such will be treated as tables without PK altogether.

## Example

postgres=# create database source_db;
CREATE DATABASE
postgres=# \c source_db 
You are now connected to database "source_db" as user "postgres".
source_db=# create table table1(a bigint primary key, b varchar, c varchar);
CREATE TABLE
source_db=# create table table2(b varchar, c varchar);
CREATE TABLE
source_db=# create table table3(a bigint, b timestamp, primary key (a,b), c varchar, d varchar);
CREATE TABLE
source_db=# \c postgres;
You are now connected to database "postgres" as user "postgres".
postgres=# create database dest_db template source_db;
CREATE DATABASE
source_db=# insert into table1 values(1, 'hello', 'world');
INSERT 0 1
source_db=# insert into table1 values(2, 'world', 'hello');
INSERT 0 1
source_db=# insert into table2 values('world', 'hello');
INSERT 0 1
source_db=# insert into table2 values('hello', 'world');
INSERT 0 1
source_db=# insert into table3 values(1, now(), 'hello', 'world');
INSERT 0 1
source_db=# insert into table3 values(2, now(), 'world', 'hello');
INSERT 0 1
source_db=# 

connecting to 127.0.0.1 5432 postgres source_db
connecting to 127.0.0.1 5432 postgres dest_db

Attempting to synchronize table table1 with strategy HASH
found primary key column a for table table1
analyzed 2 source rows, 0 destination rows
calculated 2 inserts, 0 updates, 0 deletes
COPY completed with 2 rows affected
performed 2 inserts, 0 updates, 0 deletes

Attempting to synchronize table table2 with strategy HASH
failed to determine a suitable PK. Resorting to ctid.
analyzed 2 source rows, 0 destination rows
calculated 2 inserts, 0 updates, 0 deletes
COPY completed with 2 rows affected
performed 2 inserts, 0 updates, 0 deletes

Attempting to synchronize table table3 with strategy HASH
table table3 appears to have a composite PK
failed to determine a suitable PK. Resorting to ctid.
analyzed 2 source rows, 0 destination rows
calculated 2 inserts, 0 updates, 0 deletes
COPY completed with 2 rows affected
performed 2 inserts, 0 updates, 0 deletes

source_db=# update table1 set b = 'stuff' where a = 2;
UPDATE 1
source_db=# delete from table3 where a = 1;
DELETE 1
source_db=# 

Attempting to synchronize table table1 with strategy HASH
found primary key column a for table table1
analyzed 2 source rows, 2 destination rows
calculated 0 inserts, 1 updates, 0 deletes
performed 0 inserts, 1 updates, 0 deletes

Attempting to synchronize table table2 with strategy HASH
failed to determine a suitable PK. Resorting to ctid.
analyzed 2 source rows, 2 destination rows
calculated 0 inserts, 0 updates, 0 deletes

Attempting to synchronize table table3 with strategy HASH
table table3 appears to have a composite PK
failed to determine a suitable PK. Resorting to ctid.
analyzed 1 source rows, 2 destination rows
calculated 0 inserts, 0 updates, 1 deletes
performed 0 inserts, 0 updates, 1 deletes
