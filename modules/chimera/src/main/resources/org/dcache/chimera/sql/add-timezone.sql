--
-- fix timestamp field to support TIME ZONE
--
ALTER TABLE t_inodes ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_inodes ALTER iatime TYPE timestamp with time zone;
ALTER TABLE t_inodes ALTER imtime TYPE timestamp with time zone;

ALTER TABLE t_level_1 ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_level_1 ALTER iatime TYPE timestamp with time zone;
ALTER TABLE t_level_1 ALTER imtime TYPE timestamp with time zone;

ALTER TABLE t_level_2 ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_level_2 ALTER iatime TYPE timestamp with time zone;
ALTER TABLE t_level_2 ALTER imtime TYPE timestamp with time zone;

ALTER TABLE t_level_3 ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_level_3 ALTER iatime TYPE timestamp with time zone;
ALTER TABLE t_level_3 ALTER imtime TYPE timestamp with time zone;

ALTER TABLE t_level_4 ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_level_4 ALTER iatime TYPE timestamp with time zone;
ALTER TABLE t_level_4 ALTER imtime TYPE timestamp with time zone;

ALTER TABLE t_level_5 ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_level_5 ALTER iatime TYPE timestamp with time zone;
ALTER TABLE t_level_5 ALTER imtime TYPE timestamp with time zone;

ALTER TABLE t_level_6 ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_level_6 ALTER iatime TYPE timestamp with time zone;
ALTER TABLE t_level_6 ALTER imtime TYPE timestamp with time zone;

ALTER TABLE t_level_7 ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_level_7 ALTER iatime TYPE timestamp with time zone;
ALTER TABLE t_level_7 ALTER imtime TYPE timestamp with time zone;


ALTER TABLE t_tags_inodes ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_tags_inodes ALTER iatime TYPE timestamp with time zone;
ALTER TABLE t_tags_inodes ALTER imtime TYPE timestamp with time zone;

ALTER TABLE t_locationinfo ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_locationinfo ALTER iatime TYPE timestamp with time zone;

ALTER TABLE t_locationinfo_trash ALTER ictime TYPE timestamp with time zone;
ALTER TABLE t_locationinfo_trash ALTER iatime TYPE timestamp with time zone;
