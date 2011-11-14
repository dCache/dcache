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
	 address_msk  CHAR(32) NOT NULL DEFAULT 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
	 ace_order  int4 NOT NULL DEFAULT 0,
	 PRIMARY KEY (rs_id, ace_order),
     FOREIGN KEY (rs_id) REFERENCES t_inodes( ipnfsid ) ON DELETE CASCADE
 );

CREATE INDEX i_t_acl_rs_id ON t_acl(rs_id);