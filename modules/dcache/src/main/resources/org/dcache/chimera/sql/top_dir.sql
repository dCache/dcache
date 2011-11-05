-- $Id: top_dir.sql 297 2007-10-30 21:13:05Z tigran $

-- top dir
SELECT iparent, COUNT(iparent) AS pcount FROM t_dirs GROUP BY iparent ORDER BY pcount;
-- file of user uid 3750
SELECT * FROM t_inodes WHERE iuid=3750 AND NOT (imode & 16384 = 16384 );
