CHAPTER 14. THE STATISTICS SERVICE
==================================

Table of Contents
------------------

[The Basic Setup](#the-basic-setup)  
[The Statistics Web Page](#the-statistics-web-page)  
[Explanation of the File Format of the xxx.raw Files](#explanation-of-the-file-format-of-the-xxxraw-files)  


The `statistics` service collects information on the amount of data stored on all pools and the total data flow including streams from and to tertiary storage systems.

Once per hour an ASCII file is produced, containing a table with information on the amount of used disk space and the data transferred starting midnight up to this point in time. Data is sorted per pool and storage class.

In addition to the hourly statistics, files are produced reporting on the daily, monthly and yearly dCache activities. An HTML tree is produced and updated once per hour allowing to navigate through the collected statistics information.


THE BASIC SETUP
===============

Define the statistics service in the domain, where the httpd is running.

    [httpdDomain]
    [httpdDomain/httpd]
    ...
    [httpdDomain/statistics]

The statistics service automatically creates a directory tree, structured according to years, months and days.

Once per hour, a **total.raw** file is produced underneath the active **year**, **month** and **day** directories, containing the sum over all pools and storage classes of the corresponding time interval. The **day** directory contains detailed statistics per hour and for the whole day.

    /var/lib/dcache/statistics/YYYY/total.raw
    /var/lib/dcache/statistics/YYYY/MM/total.raw
    /var/lib/dcache/statistics/YYYY/MM/DD/total.raw
    /var/lib/dcache/statistics/YYYY/MM/DD/YYYY-MM-DD-day.raw
    /var/lib/dcache/statistics/YYYY/MM/DD/YYYY-MM-DD-HH.raw

In the same directory tree the HTML files are created for each day, month and year.

    /var/lib/dcache/statistics/YYYY/index.html
    /var/lib/dcache/statistics/YYYY/MM/index.html
    /var/lib/dcache/statistics/YYYY/MM/DD/index.html

By default the path for the statistics data is **/var/lib/dcache/statistics**. You can modify this path by setting the property **dcache.paths.statistics** to a different value.

THE STATISTICS WEB PAGE
=======================

Point a web browser to your dCache webpage at http://<head-node.example.org>:2288/. On the bottom you find the link to Statistics.

The statistics data needs to be collected for a day before it will appear on the web page.

> **NOTE**
>
> You will get an error if you try to read the statistics web page right after you enabled the STATISTICS as the web page has not yet been created.
>
> Create data and the web page by logging in to the admin interface and running the commands `create
> 	stat` and `create html`.
>
>     (local) admin > cd PoolStatistics@<httpdDomain>
>     (PoolStatistics@)<httpdDomain> admin > create stat
>     Thread started for internal run
>     (PoolStatistics@)<httpdDomain> admin > create html
>     java.lang.NullPointerException

>
> Now you can see a statistics web page.

Statistics is calculated once per hour at `<HH>:55`. The daily stuff is calculated at `23:55`. Without manual intervention, it takes two midnights before all HTML statistics pages are available. There is a way to get this done after just one midnight. After the first midnight following the first startup of the statistics module, log into the `PoolStatistics` cell and run the following commands in the given sequence. The specified date has to be the Year/Month/Day of today.

    (PoolStatistics@)<httpdDomain> admin > create html <YYYY> <MM> <DD>
    done
    (PoolStatistics@)<httpdDomain> admin > create html <YYYY> <MM>
    done
    (PoolStatistics@)<httpdDomain> admin > create html <YYYY>
    done
    (PoolStatistics@)<httpdDomain> admin > create html
    done

You will see an empty statistics page athttp://<head-node.example.org>:2288/statistics/.

On the `Statistics Help Page`  http://<head-node.example.org>:2288/docs/statisticsHelp.html you find an explanation for the colors.

EXPLANATION OF THE FILE FORMAT OF THE XXX.RAW FILES
=====================================================

The file formats of the **/var/lib/dcache/statistics/YYYY/MM/DD/YYYY-MM-DD-HH.raw** and the **/var/lib/dcache/statistics/YYYY/MM/DD/YYYY-MM-DD-day.raw** files are similar. The file **/var/lib/dcache/statistics/YYYY/MM/DD/YYYY-MM-DD-HH.raw** does not contain columns 2 and 3 as these are related to the day and not to the hour.

Example:  

The file format of the **/var/lib/dcache/statistics/YYYY/MM/DD/YYYY-MM-DD-day.raw** files:

    #
    # timestamp=1361364900897 
    # date=Wed Feb 20 13:55:00 CET 2013
    #
    pool1 StoreA:GroupB@osm 21307929 10155 2466935 10155 0 925 0  0   0   0   85362 0

Format of `YYYY-MM-DD-day.raw` files.

| Column Number |                                     Column Description                                    |
|:-------------:|:-----------------------------------------------------------------------------------------:|
|       0       |                                         Pool Name                                         |
|       1       |                                       Storage Class                                       |
|       2       |    Bytes stored on this pool for this storage class at beginning of day MDASH green bar   |
|       3       |       Number of files stored on this pool for this storage class at beginning of day      |
|       4       | Bytes stored on this pool for this storage class at this hour or end of day MDASH red bar |
|       5       |   Number of files stored on this pool for this storage class at this hour or end of day   |
|       6       |                   Total Number of transfers (in and out, dCache-client)                   |
|       7       |                          Total Number of restores (HSM to dCache)                         |
|       8       |                           Total Number of stores (dCache to HSM)                          |
|       9       |                                    Total Number errors                                    |
|       10      |          Total Number of bytes transferred from client into dCache MDASH blue bar         |
|       11      |         Total Number of bytes transferred from dCache to clients MDASH yellow bar         |
|       12      |             Total Number of bytes tranferred from HSM to dCache MDASH pink bar            |
|       13      |            Total Number of bytes tranferred from dCache to HSM MDASH orange bar           |

The `YYYY/MM/DD/YYYY-MM-DD-HH.raw` files do not contain line 2 and 3.
