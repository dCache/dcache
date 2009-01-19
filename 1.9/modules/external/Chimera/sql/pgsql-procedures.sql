-- $Id: pgsql-procedures.sql 796 2008-09-05 13:40:45Z tigran $
-- some procedures to push some work to SQL server

CREATE OR REPLACE FUNCTION "public"."inode2path" (varchar) RETURNS varchar AS $$
DECLARE
     inode VARCHAR := $1;
     ipath varchar := '';
     ichain  RECORD;
BEGIN

    LOOP
        SELECT INTO ichain * FROM t_dirs WHERE ipnfsid=inode AND iname != '.' AND iname != '..';
        IF FOUND  AND ichain.iparent != inode
        THEN
            ipath :=   '/' || ichain.iname ||  ipath;
            inode := ichain.iparent;
        ELSE
            EXIT;
        END IF;

        END LOOP;

     RETURN ipath;
END;
$$
LANGUAGE 'plpgsql';


--
--  store location of deleted  inodes in trash table
--
-- stores a old values into the trash table except last access time,
-- which replaced with a time, when the trigger was running
--

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
