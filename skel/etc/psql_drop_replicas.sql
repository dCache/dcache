--
-- PostgreSQL database dump
--

SET client_encoding = 'SQL_ASCII';
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

DROP INDEX public.replicas_pnfsid_indx;
DROP INDEX public.pools_pool_indx;
DROP INDEX public.history_idx;
DROP INDEX public.history_b_idx;
DROP INDEX public.history_a_idx;
ALTER TABLE ONLY public.replicas DROP CONSTRAINT replica;
ALTER TABLE ONLY public.pools DROP CONSTRAINT poolname;
ALTER TABLE ONLY public.heartbeat DROP CONSTRAINT hbprocess;
DROP TABLE public.history_b;
DROP TABLE public.history_a;
DROP TABLE public.history;
DROP TABLE public.heartbeat;
DROP TABLE public."action";
SET search_path = proc, pg_catalog;

DROP VIEW proc.replicas;
SET search_path = public, pg_catalog;

DROP TABLE public.replicas;
DROP TABLE public.pools;
DROP SCHEMA proc;
