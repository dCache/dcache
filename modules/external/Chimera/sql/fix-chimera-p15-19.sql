--
-- fix pre 1.8.0-15p9 DB schema for chimera based installations
--


--
-- add missing ON DELETE CASCADE
--
ALTER TABLE t_inodes_checksum DROP CONSTRAINT t_inodes_checksum_ipnfsid_fkey;
ALTER TABLE ONLY t_inodes_checksum
    ADD CONSTRAINT t_inodes_checksum_ipnfsid_fkey FOREIGN KEY (ipnfsid) REFERENCES t_inodes(ipnfsid) ON DELETE CASCADE;

--
-- add missing ON DELETE CASCADE
--
ALTER TABLE t_inodes_data DROP CONSTRAINT t_inodes_data_ipnfsid_fkey;
ALTER TABLE ONLY t_inodes_data
    ADD CONSTRAINT t_inodes_data_ipnfsid_fkey FOREIGN KEY (ipnfsid) REFERENCES t_inodes(ipnfsid) ON DELETE CASCADE;

--
-- add missing ON DELETE CASCADE
--
ALTER TABLE t_locationinfo DROP CONSTRAINT t_locationinfo_ipnfsid_fkey;
ALTER TABLE ONLY t_locationinfo
    ADD CONSTRAINT t_locationinfo_ipnfsid_fkey FOREIGN KEY (ipnfsid) REFERENCES t_inodes(ipnfsid) ON DELETE CASCADE;

--
-- add missing ON DELETE CASCADE
--
ALTER TABLE t_storageinfo DROP CONSTRAINT t_storageinfo_ipnfsid_fkey;
ALTER TABLE ONLY t_storageinfo
    ADD CONSTRAINT t_storageinfo_ipnfsid_fkey FOREIGN KEY (ipnfsid) REFERENCES t_inodes(ipnfsid) ON DELETE CASCADE;;

--
-- replace BAD trigger
--
drop TRIGGER tgr_locationinfo_trash ON t_locationinfo;
CREATE OR REPLACE FUNCTION f_locationinfo2trash() RETURNS TRIGGER AS $t_inodes_trash$
BEGIN

    IF (TG_OP = 'DELETE') THEN

        INSERT INTO t_locationinfo_trash SELECT
            ipnfsid ,
            itype,
            ilocation ,
            ipriority,
            ictime ,
            iatime ,
            istate FROM t_locationinfo WHERE ipnfsid = OLD.ipnfsid;

    END IF;

    RETURN OLD;
END;

$t_inodes_trash$ LANGUAGE plpgsql;


--
-- trigger to store removed inodes
--

CREATE TRIGGER tgr_locationinfo_trash BEFORE DELETE ON t_inodes FOR EACH ROW EXECUTE PROCEDURE f_locationinfo2trash();


---
--- populate inhereted tags
---
CREATE OR REPLACE FUNCTION f_populate_tags() RETURNS TRIGGER AS $t_populate_tags$
BEGIN
	IF TG_OP = 'INSERT' AND NEW.iname = '..'
    THEN
	    INSERT INTO t_tags ( SELECT NEW.iparent, itagname, itagid, 0 from t_tags WHERE ipnfsid=NEW.ipnfsid );
    END IF;

	RETURN NEW;
END;

$t_populate_tags$ LANGUAGE plpgsql;
--
-- trigger to store removed inodes
--

CREATE TRIGGER tgr_populate_tags AFTER INSERT ON t_dirs FOR EACH ROW EXECUTE PROCEDURE f_populate_tags();

