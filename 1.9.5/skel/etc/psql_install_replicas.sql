--
-- Name: proc; Type: SCHEMA; Schema: -; Owner: dcache
--

CREATE SCHEMA proc;

--
-- Name: SCHEMA public; Type: COMMENT; Schema: -;
--

COMMENT ON SCHEMA public IS 'Standard public schema';


SET search_path = public, pg_catalog;

--
-- Name: poolids; Type: SEQUENCE; Schema: public;
--

CREATE SEQUENCE poolids
    INCREMENT BY 1
    NO MAXVALUE
    MINVALUE 10001
    CACHE 1;



SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: pools; Type: TABLE; Schema: public; 
--

CREATE TABLE pools (
    pool text NOT NULL,
    status text,
    datestamp timestamp without time zone,
    poolid integer DEFAULT nextval('poolids'::regclass) NOT NULL,
    countable boolean DEFAULT true
);



--
-- Name: replicas; Type: TABLE; Schema: public; 
--

CREATE TABLE replicas (
    pool text NOT NULL,
    pnfsid text NOT NULL,
    datestamp timestamp without time zone,
    poolid integer NOT NULL,
    bitmask integer NOT NULL,
    countable boolean NOT NULL,
    excluded boolean NOT NULL
);



SET search_path = proc, pg_catalog;

--
-- Name: replicas; Type: VIEW; Schema: proc; 
--

CREATE VIEW replicas AS
    SELECT replicas.pool, replicas.pnfsid FROM public.replicas, public.pools WHERE ((replicas.pool = pools.pool) AND (pools.status = 'online'::text));



SET search_path = public, pg_catalog;

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
-- Name: configuration; Type: TABLE; Schema: public; 
--

CREATE TABLE configuration (
    label text NOT NULL,
    value text,
    "comment" text
);



--
-- Name: deficient; Type: TABLE; Schema: public; 
--

CREATE TABLE deficient (
    pnfsid text,
    count integer
);



--
-- Name: drainoff; Type: TABLE; Schema: public; 
--

CREATE TABLE drainoff (
    pnfsid text
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
-- Name: files; Type: TABLE; Schema: public; 
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
-- Name: heartbeat; Type: TABLE; Schema: public; 
--

CREATE TABLE heartbeat (
    process text NOT NULL,
    description text,
    datestamp timestamp without time zone
);



--
-- Name: history; Type: TABLE; Schema: public; 
--

CREATE TABLE history (
    pool text NOT NULL,
    pnfsid text NOT NULL,
    datestamp timestamp without time zone,
    "timestamp" timestamp without time zone DEFAULT now()
);



--
-- Name: history_a; Type: TABLE; Schema: public; 
--

CREATE TABLE history_a (
    "old" boolean DEFAULT false
)
INHERITS (history);



--
-- Name: history_b; Type: TABLE; Schema: public; 
--

CREATE TABLE history_b (
    "old" boolean DEFAULT true
)
INHERITS (history);



--
-- Name: redundant; Type: TABLE; Schema: public; 
--

CREATE TABLE redundant (
    pnfsid text,
    count integer
);



--
-- Name: shadow; Type: TABLE; Schema: public; 
--

CREATE TABLE shadow (
)
INHERITS (replicas);



--
-- Name: files_pkey; Type: CONSTRAINT; Schema: public; 
--

ALTER TABLE ONLY files
    ADD CONSTRAINT files_pkey PRIMARY KEY (pnfsid);


--
-- Name: hbprocess; Type: CONSTRAINT; Schema: public; 
--

ALTER TABLE ONLY heartbeat
    ADD CONSTRAINT hbprocess PRIMARY KEY (process);


--
-- Name: poolname; Type: CONSTRAINT; Schema: public; 
--

ALTER TABLE ONLY pools
    ADD CONSTRAINT poolname PRIMARY KEY (pool);


--
-- Name: replica; Type: CONSTRAINT; Schema: public; 
--

ALTER TABLE ONLY replicas
    ADD CONSTRAINT replica PRIMARY KEY (pool, pnfsid);


--
-- Name: history_a_idx; Type: INDEX; Schema: public; 
--

CREATE INDEX history_a_idx ON history_a USING btree (pnfsid);


--
-- Name: history_b_idx; Type: INDEX; Schema: public; 
--

CREATE INDEX history_b_idx ON history_b USING btree (pnfsid);


--
-- Name: history_idx; Type: INDEX; Schema: public; 
--

CREATE INDEX history_idx ON history USING btree (pnfsid);


--
-- Name: poolid_idx; Type: INDEX; Schema: public; 
--

CREATE UNIQUE INDEX poolid_idx ON pools USING btree (poolid);


--
-- Name: pools_pool_indx; Type: INDEX; Schema: public; 
--

CREATE INDEX pools_pool_indx ON replicas USING btree (pool);


--
-- Name: replicas_pnfsid_indx; Type: INDEX; Schema: public; 
--

CREATE INDEX replicas_pnfsid_indx ON replicas USING btree (pnfsid);


--
-- Name: add2files; Type: RULE; Schema: public; 
--

CREATE RULE add2files AS ON INSERT TO replicas DO INSERT INTO files (pnfsid, datestamp) VALUES (new.pnfsid, now());


--
-- Name: in_files; Type: RULE; Schema: public; 
--

CREATE RULE in_files AS ON INSERT TO files WHERE (EXISTS (SELECT 1 FROM files WHERE (files.pnfsid = new.pnfsid))) DO INSTEAD NOTHING;


--
-- Name: replace_heartbeat; Type: RULE; Schema: public; 
--

CREATE RULE replace_heartbeat AS ON INSERT TO heartbeat WHERE (EXISTS (SELECT 1 FROM heartbeat WHERE (heartbeat.process = new.process))) DO INSTEAD UPDATE heartbeat SET datestamp = now() WHERE (heartbeat.process = new.process);


--
-- Name: replace_replicas; Type: RULE; Schema: public; 
--
-- More test required
--CREATE RULE replace_replicas AS ON INSERT TO replicas WHERE (EXISTS (SELECT 1 FROM replicas WHERE ((replicas.pool = new.pool) AND (replicas.pnfsid = new.pnfsid)))) DO INSTEAD UPDATE replicas SET datestamp = now() WHERE ((replicas.pool = new.pool) AND (replicas.pnfsid = new.pnfsid));
--

--
-- Name: replicas_pool_fkey; Type: FK CONSTRAINT; Schema: public; 
--

ALTER TABLE ONLY replicas
    ADD CONSTRAINT replicas_pool_fkey FOREIGN KEY (pool) REFERENCES pools(pool);


--
-- Name: replicas_poolid_fkey; Type: FK CONSTRAINT; Schema: public; 
--

ALTER TABLE ONLY replicas
    ADD CONSTRAINT replicas_poolid_fkey FOREIGN KEY (poolid) REFERENCES pools(poolid);

INSERT INTO configuration VALUES ('version','1.0','Initial version');

