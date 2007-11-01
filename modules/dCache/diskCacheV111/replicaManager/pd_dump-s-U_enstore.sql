--
-- PostgreSQL database dump
--

\connect - enstore

--
-- TOC entry 2 (OID 25168)
-- Name: proc; Type: SCHEMA; Schema: -; Owner: enstore
--

CREATE SCHEMA proc;


SET search_path = public, pg_catalog;

--
-- TOC entry 3 (OID 25169)
-- Name: replicas; Type: TABLE; Schema: public; Owner: enstore
--

CREATE TABLE replicas (
    pool text NOT NULL,
    pnfsid text NOT NULL,
    datestamp timestamp without time zone
);


--
-- TOC entry 4 (OID 25169)
-- Name: replicas; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE replicas FROM PUBLIC;


--
-- TOC entry 5 (OID 25174)
-- Name: pools; Type: TABLE; Schema: public; Owner: enstore
--

CREATE TABLE pools (
    pool text NOT NULL,
    status text,
    datestamp timestamp without time zone
);


--
-- TOC entry 6 (OID 25174)
-- Name: pools; Type: ACL; Schema: public; Owner: enstore
--

REVOKE ALL ON TABLE pools FROM PUBLIC;


SET search_path = proc, pg_catalog;

--
-- TOC entry 7 (OID 25181)
-- Name: replicas; Type: VIEW; Schema: proc; Owner: enstore
--

CREATE VIEW replicas AS
    SELECT replicas.pool, replicas.pnfsid FROM public.replicas, public.pools WHERE ((replicas.pool = pools.pool) AND (pools.status = 'online'::text));


SET search_path = public, pg_catalog;

--
-- TOC entry 8 (OID 46118)
-- Name: action; Type: TABLE; Schema: public; Owner: enstore
--

CREATE TABLE "action" (
    "timestamp" bigint
)
INHERITS (replicas);


--
-- TOC entry 9 (OID 62893)
-- Name: heartbeat; Type: TABLE; Schema: public; Owner: enstore
--

CREATE TABLE heartbeat (
    process text NOT NULL,
    description text,
    datestamp timestamp without time zone
);


--
-- TOC entry 11 (OID 25182)
-- Name: poolname; Type: CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY pools
    ADD CONSTRAINT poolname PRIMARY KEY (pool);


--
-- TOC entry 10 (OID 25184)
-- Name: replica; Type: CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY replicas
    ADD CONSTRAINT replica PRIMARY KEY (pool, pnfsid);


--
-- TOC entry 12 (OID 63435)
-- Name: hbprocess; Type: CONSTRAINT; Schema: public; Owner: enstore
--

ALTER TABLE ONLY heartbeat
    ADD CONSTRAINT hbprocess PRIMARY KEY (process);


