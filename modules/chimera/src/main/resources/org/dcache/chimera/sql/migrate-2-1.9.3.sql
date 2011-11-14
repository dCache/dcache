--
-- convert pre 1.9.2 db schema to the new one
--

--
-- create new tables and populate them
--
CREATE TABLE t_access_latency AS SELECT ipnfsid, iaccessLatency FROM t_storageinfo;
CREATE TABLE t_retention_policy AS SELECT ipnfsid, iretentionPolicy FROM t_storageinfo;


--
-- remove obsolete columns
--
ALTER TABLE t_storageinfo DROP COLUMN iretentionpolicy ;
ALTER TABLE t_storageinfo DROP COLUMN iaccesslatency ;


--
-- add foreign key:
--
--    remove entry when inode is removed
--
ALTER TABLE ONLY t_access_latency ADD CONSTRAINT t_access_latency_ipnfsid_fkey
    FOREIGN KEY (ipnfsid) REFERENCES t_inodes(ipnfsid) ON DELETE CASCADE;
ALTER TABLE ONLY t_retention_policy ADD CONSTRAINT t_retention_policy_ipnfsid_fkey
    FOREIGN KEY (ipnfsid) REFERENCES t_inodes(ipnfsid) ON DELETE CASCADE;
