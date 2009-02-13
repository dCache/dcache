--
-- Chimera table layout
-- for postgreSQL
--
-- $Id: create.sql 780 2008-07-21 17:48:42Z tigran $

--
--  Inodes
--

CREATE TABLE t_inodes (
    ipnfsid character(36) PRIMARY KEY,
    itype integer NOT NULL,
    imode integer NOT NULL,
    inlink integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    iio integer NOT NULL,
    ictime timestamp with time zone NOT NULL,
    iatime timestamp with time zone NOT NULL,
    imtime timestamp with time zone NOT NULL
);

--
-- dirs
--

CREATE TABLE t_dirs (
    iparent character(36) NOT NULL,
    iname character varying(255) NOT NULL,
    ipnfsid character(36) NOT NULL,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid ),
	PRIMARY KEY (iparent,iname)
);

--
-- data
--

CREATE TABLE t_inodes_data (
    ipnfsid character(36) PRIMARY KEY,
    ifiledata bytea,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid ) ON DELETE CASCADE
);


--
-- Checksums
--
CREATE TABLE t_inodes_checksum (
    ipnfsid character(36) PRIMARY KEY,
    itype integer NOT NULL,
    isum character varying(128) NOT NULL,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid ) ON DELETE CASCADE
);
--
-- pnfs levels 1-7
--

CREATE TABLE t_level_1 (
    ipnfsid character(36) PRIMARY KEY,
    imode integer NOT NULL,
    inlink integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    ictime timestamp with time zone NOT NULL,
    iatime timestamp with time zone NOT NULL,
    imtime timestamp with time zone NOT NULL,
    ifiledata bytea,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid )
);

CREATE TABLE t_level_2 (
    ipnfsid character(36) PRIMARY KEY,
    imode integer NOT NULL,
    inlink integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    ictime timestamp with time zone NOT NULL,
    iatime timestamp with time zone NOT NULL,
    imtime timestamp with time zone NOT NULL,
    ifiledata bytea,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid )
);

CREATE TABLE t_level_3 (
    ipnfsid character(36) PRIMARY KEY,
    imode integer NOT NULL,
    inlink integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    ictime timestamp with time zone NOT NULL,
    iatime timestamp with time zone NOT NULL,
    imtime timestamp with time zone NOT NULL,
    ifiledata bytea,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid )
);

CREATE TABLE t_level_4 (
    ipnfsid character(36) PRIMARY KEY,
    imode integer NOT NULL,
    inlink integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    ictime timestamp with time zone NOT NULL,
    iatime timestamp with time zone NOT NULL,
    imtime timestamp with time zone NOT NULL,
    ifiledata bytea,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid )
);

CREATE TABLE t_level_5 (
    ipnfsid character(36) PRIMARY KEY,
    imode integer NOT NULL,
    inlink integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    ictime timestamp with time zone NOT NULL,
    iatime timestamp with time zone NOT NULL,
    imtime timestamp with time zone NOT NULL,
    ifiledata bytea,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid )
);

CREATE TABLE t_level_6 (
    ipnfsid character(36) PRIMARY KEY,
    imode integer NOT NULL,
    inlink integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    ictime timestamp with time zone NOT NULL,
    iatime timestamp with time zone NOT NULL,
    imtime timestamp with time zone NOT NULL,
    ifiledata bytea,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid )
);

CREATE TABLE t_level_7 (
    ipnfsid character(36) PRIMARY KEY,
    imode integer NOT NULL,
    inlink integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    ictime timestamp with time zone NOT NULL,
    iatime timestamp with time zone NOT NULL,
    imtime timestamp with time zone NOT NULL,
    ifiledata bytea,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid )
);


--
-- Name: t_tags_inodes; Type: TABLE; Schema: public; Owner: postgres; Tablespace:
--

CREATE TABLE t_tags_inodes (
    itagid character(36) PRIMARY KEY,
    imode integer NOT NULL,
    inlink integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    ictime timestamp with time zone NOT NULL,
    iatime timestamp with time zone NOT NULL,
    imtime timestamp with time zone NOT NULL,
    ivalue bytea
);

--
-- Name: t_tags; Type: TABLE; Schema: public; Owner: postgres; Tablespace:
--

CREATE TABLE t_tags (
    ipnfsid character(36) NOT NULL,
    itagname character varying(255) NOT NULL,
    itagid character(36) NOT NULL,
    isorign integer NOT NULL,
	PRIMARY KEY (ipnfsid, itagname),
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid ),
	FOREIGN KEY (itagid) REFERENCES t_tags_inodes( itagid )
);

--
-- populate Inodes
--

INSERT INTO t_inodes VALUES ('F674EC8B0CFF104AA109828000696CAD6CAC',	16384, 493,	2,	0,	0,	512, 0,	NOW(), NOW(), NOW() );
INSERT INTO t_inodes VALUES ('000000000000000000000000000000000000',	16384, 493,	6,	0,	0,	512, 0,	NOW(), NOW(), NOW() );
INSERT INTO t_inodes VALUES ('A0D739870178504FF109C52075F44287F9DE',	16384, 493,	4,	0,	0,	512, 0,	NOW(), NOW(), NOW() );
INSERT INTO t_inodes VALUES ('1B3BB44C05C9904DFB0928F06F2467395CD5',	16384, 493,	6,	0,	0,	512, 1,	NOW(), NOW(), NOW() );
INSERT INTO t_inodes VALUES ('E3BB936F04F6D047A70B75201EDBA32FA9F5',	16384, 493,	2,	0,	0,	512, 1,	NOW(), NOW(), NOW() );
INSERT INTO t_inodes VALUES ('80D1B8B90CED30430608C58002811B3285FC',	16384, 493,	2,	0,	0,	512, 1,	NOW(), NOW(), NOW() );

--
-- directory
--

INSERT INTO t_dirs VALUES ('000000000000000000000000000000000000',	'.',	'000000000000000000000000000000000000');
INSERT INTO t_dirs VALUES ('000000000000000000000000000000000000',	'..',	'000000000000000000000000000000000000');
INSERT INTO t_dirs VALUES ('000000000000000000000000000000000000',	'admin','A0D739870178504FF109C52075F44287F9DE');
INSERT INTO t_dirs VALUES ('A0D739870178504FF109C52075F44287F9DE',	'.',	'A0D739870178504FF109C52075F44287F9DE');
INSERT INTO t_dirs VALUES ('A0D739870178504FF109C52075F44287F9DE',	'..',	'000000000000000000000000000000000000');
INSERT INTO t_dirs VALUES ('000000000000000000000000000000000000',	'usr',	'F674EC8B0CFF104AA109828000696CAD6CAC');
INSERT INTO t_dirs VALUES ('F674EC8B0CFF104AA109828000696CAD6CAC',	'.',	'F674EC8B0CFF104AA109828000696CAD6CAC');
INSERT INTO t_dirs VALUES ('F674EC8B0CFF104AA109828000696CAD6CAC',	'..',	'000000000000000000000000000000000000');
INSERT INTO t_dirs VALUES ('A0D739870178504FF109C52075F44287F9DE',	'etc',	'1B3BB44C05C9904DFB0928F06F2467395CD5');
INSERT INTO t_dirs VALUES ('1B3BB44C05C9904DFB0928F06F2467395CD5',	'.',	'1B3BB44C05C9904DFB0928F06F2467395CD5');
INSERT INTO t_dirs VALUES ('1B3BB44C05C9904DFB0928F06F2467395CD5',	'..',	'A0D739870178504FF109C52075F44287F9DE');
INSERT INTO t_dirs VALUES ('1B3BB44C05C9904DFB0928F06F2467395CD5',	'config','80D1B8B90CED30430608C58002811B3285FC');
INSERT INTO t_dirs VALUES ('80D1B8B90CED30430608C58002811B3285FC',	'.',	'80D1B8B90CED30430608C58002811B3285FC');
INSERT INTO t_dirs VALUES ('80D1B8B90CED30430608C58002811B3285FC',	'..',	'1B3BB44C05C9904DFB0928F06F2467395CD5');
INSERT INTO t_dirs VALUES ('1B3BB44C05C9904DFB0928F06F2467395CD5',	'exports','E3BB936F04F6D047A70B75201EDBA32FA9F5');
INSERT INTO t_dirs VALUES ('E3BB936F04F6D047A70B75201EDBA32FA9F5',	'.',	'E3BB936F04F6D047A70B75201EDBA32FA9F5');
INSERT INTO t_dirs VALUES ('E3BB936F04F6D047A70B75201EDBA32FA9F5',	'..',	'1B3BB44C05C9904DFB0928F06F2467395CD5');


--
--  For faster lookups
--

CREATE INDEX i_dirs_iparent ON t_dirs(iparent);

--
--  For faster getparent
--

CREATE INDEX i_dirs_ipnfsid ON t_dirs(ipnfsid);



-- STORAGE INFO
--
-- Generic storage information. Represents X:Y@Z + AL +RP
-- ipnfsid          : pnfsid of the inode
-- ihsmName         : Z-component of storageGroup
-- istorageGroup    : X-component of storageGroup
-- istorageSubGroup : Y-component of storageGroup
-- iaccessLatency   : ONLINE / NEARLINE ( keep on disk / keep on disk, if there is a space)
-- iretentionPolicy : CUSTODIAL / OUTPUT ( tape / disk )


CREATE TABLE t_storageinfo (
   ipnfsid CHAR(36) PRIMARY KEY,
   ihsmName VARCHAR(64) NOT NULL,
   istorageGroup VARCHAR(64) NOT NULL,
   istorageSubGroup VARCHAR(64) NOT NULL,
   FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid ) ON DELETE CASCADE
);


CREATE TABLE t_access_latency (
   ipnfsid CHAR(36) PRIMARY KEY,
   iaccessLatency INT NOT NULL,
   FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid ) ON DELETE CASCADE
);


CREATE TABLE t_retention_policy (
   ipnfsid CHAR(36) PRIMARY KEY,
   iretentionPolicy INT NOT NULL,
   FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid ) ON DELETE CASCADE
);


-- LOCATION INFO
--
-- Generic storage information
-- ipnfsid   : pnfsid of the inode
-- itype     : type of storage, e.g. tape, disk
-- ilocation : type specific information like pool name for disk and HSM connetion for tape
-- ipriority : in case of multiple locations of hint for performance or other needs
-- ictime    : location creation time
-- iatime    : last access time, probably performance killer, but nice to have for statistics
-- istate    : location status ONLINE/OFF-LINE


CREATE TABLE t_locationinfo (
	ipnfsid CHAR(36),
	itype INT NOT NULL,
	ilocation VARCHAR(1024) NOT NULL,
	ipriority INT NOT NULL,
	ictime timestamp with time zone NOT NULL,
	iatime timestamp with time zone NOT NULL,
	istate INT NOT NULL,
	FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid ) ON DELETE CASCADE,
	PRIMARY KEY (ipnfsid,itype,ilocation)
);

--
-- removed locations
-- required for HSM and pools cleanup
--

CREATE TABLE t_locationinfo_trash (
	ipnfsid CHAR(36),
	itype INT NOT NULL,
	ilocation VARCHAR(1024) NOT NULL,
	ipriority INT NOT NULL,
	ictime timestamp with time zone NOT NULL,
	iatime timestamp with time zone NOT NULL,
	istate INT NOT NULL,
	PRIMARY KEY (ipnfsid,itype,ilocation)
);

CREATE INDEX i_locationinfo_ipnfsid ON t_locationinfo(ipnfsid);

--
-- ACL table layout
-- for postgreSQL
--

CREATE TABLE t_acl (
	 rs_id CHAR(36) NOT NULL,
	 rs_type  int4 NOT NULL,
	 type  smallint NOT NULL DEFAULT 0,
	 flags  int4 NULL,
	 access_msk  int4 NOT NULL DEFAULT 0,
	 who  smallint NOT NULL,
	 who_id  int4,
	 address_msk  varchar(32) NOT NULL DEFAULT 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
	 ace_order  int4 NOT NULL DEFAULT 0,
	 PRIMARY KEY (rs_id, ace_order),
     FOREIGN KEY (rs_id) REFERENCES t_inodes( ipnfsid ) ON DELETE CASCADE
 );

 CREATE INDEX i_t_acl_rs_id ON t_acl(rs_id);