-- $Id: pgsql-procedures.sql 296 2007-10-30 21:09:30Z tigran $
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

		INSERT INTO t_locationinfo_trash (
			ipnfsid ,
    		itype,
		    ilocation ,
    		ipriority,
    		ictime ,
		    iatime ,
		    istate
			)
		VALUES(
			OLD.ipnfsid ,
    		OLD.itype,
		    OLD.ilocation ,
    		OLD.ipriority,
    		OLD.ictime ,
		    NOW() ,
		    OLD.istate			
		) ;

	END IF;

	RETURN NULL;
END;

$t_inodes_trash$ LANGUAGE plpgsql;


--
-- trigger to store removed inodes
--

CREATE TRIGGER tgr_locationinfo_trash AFTER DELETE ON t_locationinfo FOR EACH ROW EXECUTE PROCEDURE f_locationinfo2trash();
