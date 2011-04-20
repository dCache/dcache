--
-- Create configuration table
--
CREATE TABLE configuration (
    label text NOT NULL,
    value text,
    "comment" text 
);

INSERT INTO configuration VALUES ('version','1.0','Initial version');

--
-- Create a sequence for poolids
--
CREATE SEQUENCE poolids
    INCREMENT BY 1
    NO MAXVALUE
    MINVALUE 10001
    CACHE 1;

--
-- Change "pools" table
--
ALTER TABLE pools ADD poolid integer DEFAULT nextval('poolids'::regclass) NOT NULL;

ALTER TABLE pools ADD countable boolean DEFAULT true;

CREATE UNIQUE INDEX poolid_idx ON pools USING btree (poolid);

--
-- Create new "file" table
--
CREATE TABLE files (
    pnfsid text NOT NULL,
    datestamp timestamp without time zone DEFAULT now(),
    al integer,
    rp integer,
    fsize bigint,
    nmin integer,
    nmax integer,
    excluded boolean DEFAULT false,
    deleted boolean DEFAULT false,
    state integer,
    "valid" boolean DEFAULT true,
    sclass text
);

--
-- Create primary key
--
ALTER TABLE ONLY files ADD CONSTRAINT files_pkey PRIMARY KEY (pnfsid);

--
-- Create the rule to handle multiple inserts for the same pnfsid
--
CREATE OR REPLACE RULE in_files AS
        ON INSERT TO files
                WHERE (EXISTS (SELECT 1 FROM files WHERE (files.pnfsid = new.pnfsid)))
        DO INSTEAD NOTHING;

--
-- Change "replicas" table
--
ALTER TABLE replicas ADD poolid integer;

ALTER TABLE replicas ADD bitmask integer;

ALTER TABLE replicas ADD countable boolean;

ALTER TABLE replicas ADD excluded boolean;

ALTER TABLE ONLY replicas ADD CONSTRAINT replicas_pool_fkey FOREIGN KEY (pool) REFERENCES pools(pool);

ALTER TABLE ONLY replicas ADD CONSTRAINT replicas_poolid_fkey FOREIGN KEY (poolid) REFERENCES pools(poolid);

ALTER TABLE ONLY replicas ALTER poolid  SET NOT NULL;

ALTER TABLE ONLY replicas ALTER countable  SET NOT NULL;

ALTER TABLE ONLY replicas ALTER excluded  SET NOT NULL;

--
-- Two rules for replicas table.
-- NOTICE that the rules for one table are aplied in the alphabethical order!
-- So rule add2files will be aplied first...
--
CREATE OR REPLACE RULE add2files AS
        ON INSERT TO replicas
        DO ALSO
        INSERT INTO files (pnfsid, datestamp) VALUES (new.pnfsid, now());

--
-- Rule replace_replicas will be aplied second.
--        
-- More tests required
--CREATE OR REPLACE RULE replace_replicas AS
--        ON INSERT TO replicas
--                WHERE (EXISTS (SELECT 1 FROM replicas WHERE ((replicas.pool = new.pool) AND (replicas.pnfsid = new.pnfsid))))
--        DO INSTEAD
--        UPDATE replicas SET datestamp = now() WHERE ((replicas.pool = new.pool) AND (replicas.pnfsid = new.pnfsid));
--        

CREATE OR REPLACE RULE replace_heartbeat AS 
	ON INSERT TO heartbeat 
		WHERE EXISTS (SELECT 1 FROM heartbeat WHERE heartbeat.process = new.process) 
	DO INSTEAD 
	UPDATE heartbeat SET datestamp = now() WHERE (heartbeat.process = new.process);


--
-- Deficient table
--
CREATE TABLE deficient (pnfsid text, count int);

--
-- Redundant table
--
CREATE TABLE redundant (like deficient);

--
-- Drainoff table
--
CREATE TABLE drainoff (pnfsid text);


--
-- Name: actions; Type: TABLE; Schema: public; 
--
CREATE TABLE actions (
    "action" text NOT NULL,
    spool text NOT NULL,
    pnfsid text NOT NULL,
    dpool text NOT NULL,
    datestamp timestamp without time zone,
    "timestamp" bigint
);


--
-- Name: excluded; Type: TABLE; Schema: public; 
--
CREATE TABLE excluded (
    pool text NOT NULL,
    pnfsid text NOT NULL,
    datestamp timestamp without time zone,
    "timestamp" bigint,
    poolid integer NOT NULL,
    bitmask integer NOT NULL,
    errcode text,
    errmsg text
);


--
-- Drop action table inherited replicas
--
drop TABLE "action" ;

--
-- Create a new table named "shadow" nherited replicas to trick the planner
-- It does not have any data
--
CREATE TABLE shadow () INHERITS (replicas);
