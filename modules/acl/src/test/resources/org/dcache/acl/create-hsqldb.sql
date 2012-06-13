CREATE TABLE t_acl (
	 rs_id CHAR(36) NOT NULL,
	 rs_type  INT NOT NULL,
	 type  INT DEFAULT 0 NOT NULL,
	 flags INT NOT NULL,
	 access_msk  INT DEFAULT 0 NOT NULL,
	 who INT NOT NULL,
	 who_id INT,
	 address_msk  CHAR(32) DEFAULT 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' NOT NULL,
	 ace_order  INT DEFAULT 0 NOT NULL,
	 PRIMARY KEY (rs_id, ace_order)
 );

 CREATE INDEX i_t_acl_rs_id ON t_acl(rs_id);
