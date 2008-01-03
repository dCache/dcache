--
-- PostgreSQL database dump
--

SET client_encoding = 'SQL_ASCII';
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: proc; Type: SCHEMA; Schema: -; Owner: srmdcache
--

CREATE SCHEMA proc;


ALTER SCHEMA proc OWNER TO srmdcache;

--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA public IS 'Standard public schema';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: pools; Type: TABLE; Schema: public; Owner: srmdcache; Tablespace:
--

CREATE TABLE pools (
    pool text NOT NULL,
    status text,
    datestamp timestamp without time zone
);


ALTER TABLE public.pools OWNER TO srmdcache;

--
-- Name: replicas; Type: TABLE; Schema: public; Owner: srmdcache; Tablespace:
--

CREATE TABLE replicas (
    pool text NOT NULL,
    pnfsid text NOT NULL,
    datestamp timestamp without time zone
);


ALTER TABLE public.replicas OWNER TO srmdcache;

SET search_path = proc, pg_catalog;

--
-- Name: replicas; Type: VIEW; Schema: proc; Owner: srmdcache
--

CREATE VIEW replicas AS
    SELECT replicas.pool, replicas.pnfsid FROM public.replicas, public.pools WHERE ((replicas.pool = pools.pool) AND (pools.status = 'online'::text));


ALTER TABLE proc.replicas OWNER TO srmdcache;

SET search_path = public, pg_catalog;

--
-- Name: action; Type: TABLE; Schema: public; Owner: srmdcache; Tablespace:
--

CREATE TABLE "action" (
    "timestamp" bigint,
    errcode text,
    errmsg text
)
INHERITS (replicas);


ALTER TABLE public."action" OWNER TO srmdcache;

--
-- Name: heartbeat; Type: TABLE; Schema: public; Owner: srmdcache; Tablespace:
--

CREATE TABLE heartbeat (
    process text NOT NULL,
    description text,
    datestamp timestamp without time zone
);


ALTER TABLE public.heartbeat OWNER TO srmdcache;

--
-- Name: history; Type: TABLE; Schema: public; Owner: postgres; Tablespace:
--

CREATE TABLE history (
    pool text NOT NULL,
    pnfsid text NOT NULL,
    datestamp timestamp without time zone,
    "timestamp" timestamp without time zone DEFAULT now()
);


ALTER TABLE public.history OWNER TO postgres;

--
-- Name: history_a; Type: TABLE; Schema: public; Owner: postgres; Tablespace:
--

CREATE TABLE history_a (
    "old" boolean DEFAULT false
)
INHERITS (history);


ALTER TABLE public.history_a OWNER TO postgres;

--
-- Name: history_b; Type: TABLE; Schema: public; Owner: postgres; Tablespace:
--

CREATE TABLE history_b (
    "old" boolean DEFAULT true
)
INHERITS (history);


ALTER TABLE public.history_b OWNER TO postgres;

--
-- Name: hbprocess; Type: CONSTRAINT; Schema: public; Owner: srmdcache; Tablespace:
--

ALTER TABLE ONLY heartbeat
    ADD CONSTRAINT hbprocess PRIMARY KEY (process);


--
-- Name: poolname; Type: CONSTRAINT; Schema: public; Owner: srmdcache; Tablespace:
--

ALTER TABLE ONLY pools
    ADD CONSTRAINT poolname PRIMARY KEY (pool);


--
-- Name: replica; Type: CONSTRAINT; Schema: public; Owner: srmdcache; Tablespace:
--

ALTER TABLE ONLY replicas
    ADD CONSTRAINT replica PRIMARY KEY (pool, pnfsid);


--
-- Name: history_a_idx; Type: INDEX; Schema: public; Owner: postgres; Tablespace:
--

CREATE INDEX history_a_idx ON history_a USING btree (pnfsid);


--
-- Name: history_b_idx; Type: INDEX; Schema: public; Owner: postgres; Tablespace:
--

CREATE INDEX history_b_idx ON history_b USING btree (pnfsid);


--
-- Name: history_idx; Type: INDEX; Schema: public; Owner: postgres; Tablespace:
--

CREATE INDEX history_idx ON history USING btree (pnfsid);


--
-- Name: pools_pool_indx; Type: INDEX; Schema: public; Owner: srmdcache; Tablespace:
--

CREATE INDEX pools_pool_indx ON replicas USING btree (pool);


--
-- Name: replicas_pnfsid_indx; Type: INDEX; Schema: public; Owner: srmdcache; Tablespace:
--

CREATE INDEX replicas_pnfsid_indx ON replicas USING btree (pnfsid);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- Name: pools; Type: ACL; Schema: public; Owner: srmdcache
--

REVOKE ALL ON TABLE pools FROM PUBLIC;
REVOKE ALL ON TABLE pools FROM srmdcache;
GRANT ALL ON TABLE pools TO srmdcache;


--
-- Name: replicas; Type: ACL; Schema: public; Owner: srmdcache
--

REVOKE ALL ON TABLE replicas FROM PUBLIC;
REVOKE ALL ON TABLE replicas FROM srmdcache;
GRANT ALL ON TABLE replicas TO srmdcache;


--
-- PostgreSQL database dump complete
--

