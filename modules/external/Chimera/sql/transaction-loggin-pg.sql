--
-- $Id: transaction-loggin-pg.sql 296 2007-10-30 21:09:30Z tigran $
-- transaction logging
--

CREATE TABLE t_inodes_actionlog (
    iaction varchar(64) NOT NULL,
    ittime timestamp without time zone NOT NULL,
    ipnfsid character(36) NOT NULL,
    itype_old integer ,
    itype_new integer NOT NULL,
    imode_old integer ,
    imode_new integer NOT NULL,
    inlink_old integer ,
    inlink_new integer NOT NULL,
    iuid_old integer ,
    iuid_new integer NOT NULL,    
    igid_old integer ,
    igid_new integer NOT NULL,    
    isize_old bigint ,
    isize_new bigint NOT NULL,
    iio_old integer ,
    iio_new integer NOT NULL,
    ictime_old timestamp without time zone ,
    ictime_new timestamp without time zone NOT NULL,
    iatime_old timestamp without time zone ,
    iatime_new timestamp without time zone NOT NULL,
    imtime_old timestamp without time zone ,
    imtime_new timestamp without time zone NOT NULL
);



CREATE OR REPLACE FUNCTION f_actionlog() RETURNS TRIGGER AS $t_actionlog$
BEGIN
	
    IF (TG_OP = 'UPDATE') THEN

        INSERT INTO t_inodes_actionlog (
		    iaction,
		    ittime,
		    ipnfsid,
		    itype_old,
		    itype_new,
		    imode_old,
		    imode_new,
		    inlink_old,
		    inlink_new,
		    iuid_old,
		    iuid_new,    
		    igid_old,
		    igid_new,    
		    isize_old,
		    isize_new,
		    iio_old,
		    iio_new,
		    ictime_old,
		    ictime_new,
		    iatime_old,
		    iatime_new,
		    imtime_old,
		    imtime_new
        ) VALUES (
            'UPDATE',
            NOW(),
            NEW.ipnfsid,
            OLD.itype,
            NEW.itype,
            OLD.imode,
            NEW.imode,
            OLD.inlink,
            NEW.inlink,
            OLD.iuid,
            NEW.iuid,
            OLD.igid,
            NEW.igid,
            OLD.isize,
            NEW.isize,
            OLD.iio,
            NEW.iio,
            OLD.ictime,
            NEW.ictime,
            OLD.iatime,
            NEW.iatime,
            OLD.imtime,
            NEW.imtime
        );        

    ELSIF (TG_OP = 'INSERT') THEN

        INSERT INTO t_inodes_actionlog (
		    iaction,
		    ittime,
		    ipnfsid,
		    itype_new,
		    imode_new,
		    inlink_new,
		    iuid_new,    
		    igid_new,    
		    isize_new,
		    iio_new,
		    ictime_new,
		    iatime_new,
		    imtime_new
        ) VALUES (
            'INSERT',
            NOW(),
            NEW.ipnfsid,
            NEW.itype,
            NEW.imode,
            NEW.inlink,
            NEW.iuid,    
            NEW.igid,    
            NEW.isize,
            NEW.iio,
            NEW.ictime,
            NEW.iatime,
            NEW.imtime
        );

    END IF;
    
    RETURN NEW;
END ;

$t_actionlog$ LANGUAGE plpgsql;


CREATE TRIGGER tgr_actionlog BEFORE INSERT OR UPDATE ON t_inodes FOR EACH ROW EXECUTE PROCEDURE f_actionlog();