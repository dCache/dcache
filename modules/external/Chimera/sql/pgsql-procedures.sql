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

CREATE OR REPLACE FUNCTION path2inode(root varchar, path varchar) RETURNS varchar AS $$
DECLARE
    id varchar := root;
    elements varchar[] := string_to_array(path, '/');
    child varchar;
    itype integer;
    link varchar;
BEGIN
    FOR i IN 1..array_upper(elements,1) LOOP
        SELECT dir.ipnfsid, inode.itype INTO child, itype FROM t_dirs dir, t_inodes inode WHERE dir.ipnfsid = inode.ipnfsid AND dir.iparent=id AND dir.iname=elements[i];
        IF itype=40960 THEN
           SELECT ifiledata INTO link FROM t_inodes_data WHERE ipnfsid=child;
           IF link LIKE '/%' THEN
              child := path2inode('000000000000000000000000000000000000',
                                   substring(link from 2));
           ELSE
              child := path2inode(id, link);
           END IF;
        END IF;
        IF child IS NULL THEN
           RETURN NULL;
        END IF;
        id := child;
    END LOOP;
    RETURN id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
    path2inodes(root varchar, path varchar, OUT inode t_inodes)
RETURNS SETOF t_inodes AS $$
DECLARE
    dir varchar;
    elements text[] := string_to_array(path, '/');
    inodes t_inodes[];
    link varchar;
BEGIN
    -- Find the inode of the root
    SELECT * INTO inode FROM t_inodes WHERE ipnfsid = root;
    IF NOT FOUND THEN
       RETURN;
    END IF;

    -- We build an array of the inodes for the path
    inodes := ARRAY[inode];

    -- For each path element
    FOR i IN 1..array_upper(elements,1) LOOP
        -- Return empty set if not a directory
        IF inode.itype != 16384 THEN
            RETURN;
        END IF;

        -- The PNFS ID of the directory
        dir := inode.ipnfsid;

        -- Lookup the next path element
        SELECT t_inodes.* INTO inode
           FROM t_inodes, t_dirs
           WHERE t_inodes.ipnfsid = t_dirs.ipnfsid
                 AND t_dirs.iparent = dir AND iname = elements[i];

        -- Return the empty set if not found
        IF NOT FOUND THEN
           RETURN;
        END IF;

        -- Append the inode to the result set
        inodes := array_append(inodes, inode);

        -- If inode is a symbolic link
        IF inode.itype = 40960 THEN
            -- Read the link
            SELECT ifiledata INTO STRICT link
                FROM t_inodes_data WHERE ipnfsid = inode.ipnfsid;

            -- If absolute path then resolve from the file system root
            IF link LIKE '/%' THEN
               dir := '000000000000000000000000000000000000';
               link := substring(link from 2);

               -- Call recursively and add inodes to result set
               FOR inode IN SELECT * FROM path2inodes(dir, link) LOOP
                   inodes := array_append(inodes, inode);
               END LOOP;
            ELSE 
               -- Call recursively and add inodes to result set; skip 
               -- first inode as it is the inode of dir
               FOR inode IN SELECT * FROM path2inodes(dir, link) OFFSET 1 LOOP
                   inodes := array_append(inodes, inode);
               END LOOP;
            END IF;

            -- Return empty set if link could not be resolved
            IF NOT FOUND THEN
               RETURN;
            END IF;

            -- Continue from the inode pointed to by the link
            inode = inodes[array_upper(inodes,1)];
        END IF;
    END LOOP;

    -- Output all inodes
    FOR i IN 1..array_upper(inodes,1) LOOP
        inode := inodes[i];
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

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

--
-- ********  ACL in dCache  **********
--

-------------------------------------------------------------------------------
--  trigger to inherit ACLs for newly created file/directory

-------------------------------------------------------------------------------
-- optimized by mdavid
--
CREATE SEQUENCE serial MINVALUE 0;

CREATE OR REPLACE FUNCTION f_insertACL() RETURNS trigger AS $$
DECLARE
    msk INTEGER;
    flag INTEGER;
    rstype INTEGER;
    id character(36);
    parentid character(36);

BEGIN
    IF (TG_OP = 'INSERT') THEN
        msk := 0;
        SELECT INTO rstype itype FROM t_inodes WHERE ipnfsid = NEW.ipnfsid;

        IF rstype = 32768  THEN
            id := NEW.ipnfsid;
            parentid := NEW.iparent;
            rstype := 1;    -- inserted object is a file
            flag := 1;      -- check flags for 'f' bit
            msk := 11;      -- mask contains 'o','d' and 'f' bits

        ELSIF (rstype = 16384 AND NEW.iname = '..') THEN
            id := NEW.iparent;
            parentid := NEW.ipnfsid;
            rstype := 0;    -- inserted object is a directory
            flag := 3;      -- check flags for 'd' and 'f' bits
            msk := 8;       -- mask contains 'o' bit
        END IF;

        IF msk > 0 THEN
            ALTER SEQUENCE serial START 0;

            INSERT INTO t_acl
            SELECT id, rstype, type, (flags | msk) # msk, access_msk, who, who_id, address_msk, nextval('serial')
            FROM t_acl
            WHERE  rs_id = parentid AND (flags & flag > 0)
            ORDER BY ace_order;
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER  tgr_insertACL AFTER INSERT ON  t_dirs FOR EACH ROW EXECUTE PROCEDURE  f_insertACL();
