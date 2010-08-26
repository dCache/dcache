CREATE TABLE cacheinfo (
 pnfsid CHAR(24)  NOT NULL,
 pool VARCHAR(255)  NOT NULL,
 ctime TIMESTAMP NOT NULL,
 UNIQUE(pnfsid, pool)
);

CREATE INDEX pool_inx ON cacheinfo (pool);
CREATE INDEX pnfs_inx ON cacheinfo (pnfsid);
