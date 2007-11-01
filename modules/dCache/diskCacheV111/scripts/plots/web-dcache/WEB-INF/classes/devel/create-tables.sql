-- Clean up first
DROP TABLE "plots";
DROP TABLE "yy_set";
DROP TABLE "mm_set";
DROP TABLE "ww_set";
DROP TABLE "lhist_set";
DROP TABLE "yy_brd";
DROP TABLE "yy_bwr";
DROP TABLE "yy_trd";
DROP TABLE "yy_twr";
DROP TABLE "yy_hits";
DROP TABLE "yy_cost";
DROP TABLE "mm_brd";
DROP TABLE "mm_bwr";
DROP TABLE "mm_trd";
DROP TABLE "mm_twr";
DROP TABLE "mm_hits";
DROP TABLE "mm_cost";
DROP TABLE "ww_brd";
DROP TABLE "ww_bwr";
DROP TABLE "ww_trd";
DROP TABLE "ww_twr";
DROP TABLE "ww_hits";
DROP TABLE "ww_cost";
DROP TABLE "ratio";
DROP TABLE "tcost";


CREATE TABLE "plots" (
	"name" text,
	"gtitle" text,
	"xlabel" text,
	"ylabel" text,
	"settings" name,
	"datasource" name,
	"ref" name,
	"viewlevel" integer
);

-- COPY plots (name, gtitle, xlabel, ylabel, settings, datasource, ref, viewlevel) FROM stdin with delimiter ',';
COPY plots FROM stdin using delimiters ',';
yy_brd,Bytes read,Date,Bytes,yy_set,yy_brd,\N, 0
yy_bwr,Bytes written,Date,Bytes,yy_set,yy_bwr,\N, 0
yy_trd,Read transfers,Date,Transfers,yy_set,yy_trd,\N, 0
yy_twr,Write transfers,Date,Transfer,yy_set,yy_twr,\N, 0
yy_hits,Cache hits,Date,# Hits,yy_set,yy_hits,\N, 0
yy_cost,Transaction cost,Date,Cost,yy_set,yy_cost,\N, 1
\.

-- COPY plots (name, gtitle, xlabel, ylabel, settings, datasource, ref, viewlevel) FROM stdin with delimiter ',';
COPY plots FROM stdin using delimiters ',';
mm_brd,Bytes read,Date,Bytes,mm_set,mm_brd,\N, 0
mm_bwr,Bytes written,Date,Bytes,mm_set,mm_bwr,\N, 0
mm_trd,Read transfers,Date,Transfers,mm_set,mm_trd,\N, 0
mm_twr,Write transfers,Date,Transfer,mm_set,mm_twr,\N, 0
mm_hits,Cache hits,Date,# Hits,mm_set,mm_hits,\N, 0
mm_cost,Transaction cost,Date,Cost,mm_set,mm_cost,\N, 1
\.

-- COPY plots (name, gtitle, xlabel, ylabel, settings, datasource, ref, viewlevel) FROM stdin with delimiter ',';
COPY plots FROM stdin using delimiters ',';
ww_brd,Bytes read,Date,Bytes,ww_set,ww_brd,ratio, 0
ww_bwr,Bytes written,Date,Bytes,ww_set,ww_bwr,\N, 0
ww_trd,Read transfers,Date,Transfers,ww_set,ww_trd,\N, 0
ww_twr,Write transfers,Date,Transfer,ww_set,ww_twr,\N, 0
ww_hits,Cache hits,Date,# Hits,ww_set,ww_hits,\N, 0
ww_cost,Transaction cost,Date,Cost,ww_set,ww_cost,tcost, 1
\.

-- COPY plots (name, gtitle, xlabel, ylabel, settings, datasource, ref, viewlevel) FROM stdin with delimiter ',';
COPY plots FROM stdin using delimiters ',';
ratio,Transfer size/Full size,Ratio,\N,lhist_set,ratio,\N, 0
tcost,Transfer Cost Distribution,Cost,\N,lhist_set,tcost,\N, 0
\.



CREATE TABLE "yy_set" (
	"set" text
);

-- COPY yy_set (set) FROM stdin;
COPY yy_set FROM stdin;
set size 1.5,1.5
set terminal postscript eps color solid 'Arial' 24
set xlabel '$getXlabel()'
set timefmt '%Y-%m-%d'
set xdata time
set xrange ['$getSyear()':'$getEyear()']
set grid
set nolog y
set yrange [*:*]
set format x '%m/%d'
set output '$getName().eps'
set title '$getGtitle()'
set timestamp "%d/%m/%y %H:%M" top 0,-2
set ylabel '$getYlabel()'
\.

CREATE TABLE "mm_set" (
	"set" text
);

-- COPY mm_set (set) FROM stdin;
COPY mm_set FROM stdin;
set size 1.5,1.5
set terminal postscript eps color solid 'Arial' 24
set xlabel '$getXlabel()'
set timefmt '%Y-%m-%d'
set xdata time
set xrange ['$getSmonth()':'$getEmonth()']
set grid
set nolog y
set yrange [*:*]
set format x '%m/%d'
set output '$getName().eps'
set title '$getGtitle()'
set timestamp '%d/%m/%y %H:%M' top 0,-2
set ylabel '$getYlabel()'
\.

CREATE TABLE "ww_set" (
	"set" text
);

-- COPY ww_set (set) FROM stdin;
COPY ww_set FROM stdin;
set size 1.5,1.5
set terminal postscript eps color solid 'Arial' 24
set xlabel '$getXlabel()'
set timefmt '%Y-%m-%d-%H'
set xdata time
set xrange ['$getSweek()':'$getEweek()']
set grid
set nolog y
set yrange [*:*]
set format x '%m/%d'
set output '$getName().eps' 
set title '$getGtitle()'
set timestamp '%d/%m/%y %H:%M' top 0,-2
set ylabel '$getYlabel()'
\.

CREATE TABLE "lhist_set" (
	"set" text
);

-- COPY lhist_set (set) FROM stdin;
COPY lhist_set FROM stdin;
set size 1.5,1.5
set terminal postscript eps color solid 'Arial' 24
set xlabel '$getXlabel()'
set xrange [*:*]
set grid
set log y
set yrange [1:*]
set output '$getName().eps'
set title '$getGtitle()'
set timestamp '%d/%m/%y %H:%M' top 0,-2
set ylabel '$getYlabel()'
\.

CREATE TABLE yy_brd (
	"dataset" text,
	"source" name
);
COPY yy_brd FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 5 1,v_dc_brd_daily
'-' using 1:(-$2) t 'Enstore' with imp lw 5 3,v_en_brd_daily
\.

CREATE TABLE yy_bwr (
	"dataset" text,
	"source" name
);
COPY yy_bwr FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 5 1,v_dc_bwr_daily
'-' using 1:(-$2) t 'Enstore' with imp lw 5 3,v_en_bwr_daily
\.

CREATE TABLE yy_trd (
	"dataset" text,
	"source" name
);
COPY yy_trd FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 5 1,v_dc_trd_daily
'-' using 1:(-$2) t 'Enstore' with imp lw 5 3,v_en_trd_daily
\.

CREATE TABLE yy_twr (
	"dataset" text,
	"source" name
);
COPY yy_twr FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 5 1,v_dc_twr_daily
'-' using 1:(-$2) t 'Enstore' with imp lw 5 3,v_en_twr_daily
\.

CREATE TABLE yy_hits (
	"dataset" text,
	"source" name
);
COPY yy_hits FROM stdin using delimiters ',';
'-' using 1:2 t 'Cached' with histeps lw 3 1,v_hits_daily1
'-' using 1:2 t 'Not Cached' with histeps lw 3 3,v_hits_daily0
\.

CREATE TABLE yy_cost (
	"dataset" text,
	"source" name
);
COPY yy_cost FROM stdin using delimiters ',';
'-' using 1:2 t 'Cost' with linespoints lw 4 1,v_cost_daily
\.

CREATE TABLE mm_brd (
	"dataset" text,
	"source" name
);
COPY mm_brd FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 50 1,v_dc_brd_daily
'-' using 1:(-$2) t 'Enstore' with imp lw 50 3,v_en_brd_daily
\.

CREATE TABLE mm_bwr (
	"dataset" text,
	"source" name
);
COPY mm_bwr FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 50 1,v_dc_bwr_daily
'-' using 1:(-$2) t 'Enstore' with imp lw 50 3,v_en_bwr_daily
\.

CREATE TABLE mm_trd (
	"dataset" text,
	"source" name
);
COPY mm_trd FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 50 1,v_dc_trd_daily
'-' using 1:(-$2) t 'Enstore' with imp lw 50 3,v_en_trd_daily
\.

CREATE TABLE mm_twr (
	"dataset" text,
	"source" name
);
COPY mm_twr FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 50 1,v_dc_twr_daily
'-' using 1:(-$2) t 'Enstore' with imp lw 50 3,v_en_twr_daily
\.

CREATE TABLE mm_hits (
	"dataset" text,
	"source" name
);
COPY mm_hits FROM stdin using delimiters ',';
'-' using 1:2 t 'Cached' with histeps lw 3 1,v_hits_daily1
'-' using 1:2 t 'Not Cached' with histeps lw 3 3,v_hits_daily0
\.

CREATE TABLE mm_cost (
	"dataset" text,
	"source" name
);
COPY yy_cost FROM stdin using delimiters ',';
'-' using 1:2 t 'Cost' with linespoints lw 4 1,v_cost_daily
\.

CREATE TABLE ww_brd (
	"dataset" text,
	"source" name
);
COPY ww_brd FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 5 1,v_dc_brd_hourly
'-' using 1:(-$2) t 'Enstore' with imp lw 5 3,v_en_brd_hourly
\.

CREATE TABLE ww_bwr (
	"dataset" text,
	"source" name
);
COPY ww_bwr FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 5 1,v_dc_bwr_hourly
'-' using 1:(-$2) t 'Enstore' with imp lw 5 3,v_en_bwr_hourly
\.

CREATE TABLE ww_trd (
	"dataset" text,
	"source" name
);
COPY ww_trd FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 5 1,v_dc_trd_hourly
'-' using 1:(-$2) t 'Enstore' with imp lw 5 3,v_en_trd_hourly
\.

CREATE TABLE ww_twr (
	"dataset" text,
	"source" name
);
COPY ww_twr FROM stdin using delimiters ',';
'-' using 1:2 t 'dCache' with imp lw 5 1,v_dc_twr_hourly
'-' using 1:(-$2) t 'Enstore' with imp lw 5 3,v_en_twr_hourly
\.

CREATE TABLE ww_hits (
	"dataset" text,
	"source" name
);
COPY yy_hits FROM stdin using delimiters ',';
'-' using 1:2 t 'Cached' with histeps lw 3 1,v_hits_hourly1
'-' using 1:2 t 'Not Cached' with histeps lw 3 3,v_hits_hourly0
\.

CREATE TABLE ww_cost (
	"dataset" text,
	"source" name
);
COPY yy_cost FROM stdin using delimiters ',';
'-' using 1:2 t 'Cost' with linespoints lw 4 1,v_cost_hourly
\.

CREATE TABLE ratio (
	"dataset" text,
	"source" name
);
COPY ratio FROM stdin using delimiters ',';
'-' using 1:2 t 'Transfer size/Full size' with imp lw 10 1,v_ratio
\.

CREATE TABLE tcost (
	"dataset" text,
	"source" name
);
COPY tcost FROM stdin using delimiters ',';
'-' using 1:2 t 'Transfer cost' with imp lw 16 1,v_cost
\.
