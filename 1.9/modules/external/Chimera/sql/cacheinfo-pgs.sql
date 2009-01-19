

DROP TABLE cacheinfo;
DROP INDEX pool_inx;
DROP INDEX pnfs_inx;
 
 
 
CREATE TABLE cacheinfo (
  pnfsid CHAR(24)  NOT NULL,
  pool VARCHAR(255)  NOT NULL,
  ctime TIMESTAMP NOT NULL,
  UNIQUE(pnfsid, pool)
);
 
create index pool_inx on cacheinfo (pool);
create index pnfs_inx on cacheinfo (pnfsid);

