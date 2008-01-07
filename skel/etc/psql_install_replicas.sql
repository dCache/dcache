CREATE TABLE pools (
    pool text NOT NULL,
    status text,
    datestamp timestamp without time zone
);


CREATE TABLE replicas (
    pool text NOT NULL,
    pnfsid text NOT NULL,
    datestamp timestamp without time zone
);


CREATE SCHEMA proc;

SET search_path = proc, pg_catalog;

CREATE VIEW replicas AS
    SELECT replicas.pool, replicas.pnfsid FROM public.replicas, public.pools WHERE ((replicas.pool = pools.pool) AND (pools.status = 'online'::text));


SET search_path = public, pg_catalog;

CREATE TABLE "action" (
    "timestamp" bigint,
    errcode text,
    errmsg text
)
INHERITS (replicas);


CREATE TABLE heartbeat (
    process text NOT NULL,
    description text,
    datestamp timestamp without time zone
);


CREATE TABLE history (
    pool text NOT NULL,
    pnfsid text NOT NULL,
    datestamp timestamp without time zone,
    "timestamp" timestamp without time zone DEFAULT now()
);


CREATE TABLE history_a (
    "old" boolean DEFAULT false
)
INHERITS (history);


CREATE TABLE history_b (
    "old" boolean DEFAULT true
)
INHERITS (history);


ALTER TABLE ONLY heartbeat
    ADD CONSTRAINT hbprocess PRIMARY KEY (process);


ALTER TABLE ONLY pools
    ADD CONSTRAINT poolname PRIMARY KEY (pool);


ALTER TABLE ONLY replicas
    ADD CONSTRAINT replica PRIMARY KEY (pool, pnfsid);


CREATE INDEX history_a_idx ON history_a USING btree (pnfsid);


CREATE INDEX history_b_idx ON history_b USING btree (pnfsid);


CREATE INDEX history_idx ON history USING btree (pnfsid);


CREATE INDEX pools_pool_indx ON replicas USING btree (pool);


CREATE INDEX replicas_pnfsid_indx ON replicas USING btree (pnfsid);

