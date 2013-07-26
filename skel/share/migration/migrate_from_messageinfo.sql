------------------ MIGRATION SQL FOR POPULATING DAILY TABLES -------------------
--- Run this sql after adding the daily tables if there is pre-existing data
--- in the billing message tables:
---
--- billinginfo
--- storageinfo
--- costinfo
--- hitinfo
--------------------------------------------------------------------------------

INSERT into billinginfo_rd_daily (transferred, size, count, date) SELECT SUM(transfersize),
 SUM(fullsize), COUNT(datestamp), (DATE(datestamp)) from billinginfo
 where isnew ='f' and errorcode = 0 group by DATE(datestamp) ;

INSERT into billinginfo_wr_daily (transferred, size, count, date) SELECT SUM(transfersize),
 SUM(fullsize), COUNT(datestamp), (DATE(datestamp)) from billinginfo
 where isnew ='t' and errorcode = 0 group by DATE(datestamp) ;

INSERT into storageinfo_rd_daily (size, count, date) SELECT SUM(fullsize), COUNT(datestamp),
 (DATE(datestamp)) from storageinfo
 where action ='restore' and errorcode = 0 group by DATE(datestamp) ;

INSERT into storageinfo_wr_daily (size, count, date) SELECT SUM(fullsize), COUNT(datestamp),
 (DATE(datestamp)) from storageinfo
 where action ='store' and errorcode = 0 group by DATE(datestamp) ;

INSERT into billinginfo_tm_daily (maximum, minimum, totaltime, count, date)
 SELECT MAX(connectiontime), MIN(connectiontime), SUM(connectiontime),
 COUNT(datestamp), (DATE(datestamp))
 from billinginfo where errorcode = 0 group by DATE(datestamp) ;

INSERT into costinfo_daily (totalcost, count, date) SELECT SUM(cost), COUNT(datestamp),
 (DATE(datestamp)) from costinfo where errorcode = 0 group by DATE(datestamp) ;

CREATE TEMPORARY TABLE temp_hitinfo_daily (cached bigint, ncached bigint, total bigint, date timestamp);

INSERT into temp_hitinfo_daily (cached, ncached, total, date)
 SELECT COUNT(filecached), 0, COUNT(filecached), DATE(datestamp) from hitinfo
 where filecached = true and errorcode = 0 group by DATE(datestamp);

INSERT into temp_hitinfo_daily (cached, ncached, total, date)
 SELECT 0, COUNT(filecached), COUNT(filecached), DATE(datestamp) from hitinfo
 where filecached = false and errorcode = 0 group by DATE(datestamp);

INSERT into hitinfo_daily (cached, notcached, count, date)
 SELECT SUM(cached), SUM(ncached), SUM(total), date from temp_hitinfo_daily group by date;

DROP TABLE temp_hitinfo_daily;
