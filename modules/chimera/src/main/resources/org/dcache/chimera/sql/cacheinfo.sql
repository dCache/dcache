
DROP TABLE IF EXISTS cacheinfo;

CREATE TABLE cacheinfo (
  pnfsid CHAR(24)  NOT NULL ,
  pool VARCHAR(255)  NOT NULL,
  ctime TIMESTAMP NOT NULL,
  INDEX(pnfsid),
  INDEX(pool),
  UNIQUE(pnfsid,pool)
);
