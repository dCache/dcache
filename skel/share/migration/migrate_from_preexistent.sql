------------------ MIGRATION SQL FOR POPULATING DAILY TABLES -------------------
--- Run this sql after adding the daily tables if there are already daily tables
--- in the database used by the standalone servlet:
---
--- dc_rd_daily
--- dc_wr_daily
--- en_rd_daily
--- en_wr_daily
--- dc_tm_daily
--- cost_daily
--- hits_daily
--------------------------------------------------------------------------------

INSERT into billinginfo_rd_daily (transferred, size, count, date)
 SELECT transfersize, fullsize, count, date from dc_rd_daily ;

INSERT into billinginfo_wr_daily (transferred, size, count, date)
 SELECT transfersize, fullsize, count, date from dc_wr_daily ;

INSERT into storageinfo_rd_daily (size, count, date)
 SELECT fullsize, count, date from en_rd_daily ;

INSERT into storageinfo_wr_daily (size, count, date)
 SELECT fullsize, count, date from en_wr_daily ;

INSERT into billinginfo_tm_daily (maximum, minimum, totaltime, count, date)
 SELECT 1000*max, 1000*min, 1000*avg*count, count, date from dc_tm_daily;

INSERT into costinfo_daily (totalcost, count, date)
 SELECT mean*count, count, date from cost_daily;

INSERT into hitinfo_daily (cached, notcached, count, date)
 SELECT cached, notcached, count, date from hits_daily;
