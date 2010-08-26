

CREATE TABLE t_pnfsid_mapping (
	ipnfsid CHAR(24) PRIMARY KEY,
	ichimeraid CHAR(36) UNIQUE NOT NULL
);
