Chapter 24. PostgreSQL and dCache
=================================

PostgreSQL is used for various things in a dCache system: The [SRM](rf-glossary.md#storage-resource-manager-srm), the [pin manager](rf-glossary.md#pin-manager), the [space manager](rf-glossary.md#space-manager), the [replica manager](rf-glossary.md#replica-manager), the [billing](rf-glossary.md#billing), and the pnfs server might make use of one or more databases in single or several separate PostgreSQL servers.

The `SRM`, the pin manager, the space manager and the replica manager will use the PostgreSQL database as configured at cell start-up in the corresponding batch files. The `billing` will only write the accounting information into a database if it is configured with the option `-useSQL`. The `pnfs` server will use a PostgreSQL server if the `pnfs-posgresql` version is used. It will use several databases in the PostgreSQL server.

-----
[TOC bullet hierarchy]
-----

## Installing a PSQL Server

The preferred way to set up a PSQL server should be the installation of the version provided by your OS distribution; however, version 9.5 or later is required. Running the latest version
is recommended, as usually they provide better performance and stability.

Install the PSQL server, client and JDBC support with the tools of the operating system.

Initialize the database directory (for PSQL version 9.5 this is
`/var/lib/pgsql/9.5/data/`), start the database server, and make sure
that it is started at system start-up.

```console-root
service postgresql-9.5 initdb
|Initializing database:                  [  OK  ]
service postgresql-9.5 start
|Starting postgresql-9.5 service:        [  OK  ]
chkconfig postgresql-9.5 on
```

## Configuring Access to PSQL

In the installation guide instructions are given for configuring one
PSQL server on the admin node for all the above described purposes
with generous access rights. This is done to make the installation as
easy as possible. The access rights are configured in the file
`<database_directory_name>/data/pg_hba.conf`. According to the
installation guide the end of the file should look like

    ...
    # TYPE  DATABASE    USER        IP-ADDRESS        IP-MASK           METHOD
    local   all         all                                             trust
    host    all         all         127.0.0.1/32                        trust
    host    all         all         ::1/128                             trust
    host    all         all         HostIP/32          trust

This gives access to all databases in the PSQL server to all users on the admin host.

The databases can be secured by restricting access with this file. E.g.

    ...
    # TYPE  DATABASE    USER        IP-ADDRESS        METHOD
    local   all         postgres                      ident sameuser
    local   all         dcache                    password
    local   all         all                           md5
    host    all         all         127.0.0.1/32      md5
    host    all         all         ::1/128           md5
    host    all         all         HostIP/32          md5

To make the server aware of this you need to reload the configuration file as the user `postgres` by:

```console-root
su -s `which pg_ctl` postgres reload
```

The password the the user `dcache` can be set with

    [postgres] # psql template1 -c "ALTER USER dcache WITH PASSWORD '<yourPassword>'"

## Performance of the PostgreSQL Server

On small systems it should never be a problem to use one single PSQL server for all the functions listed above. In the standard installation, the `ReplicaManager` is not activated by default. The `billing` will only write to a file by default.

Whenever the PostgreSQL server is going to be used for another functionality, the impact on performance should be checked carefully. To improve the performance, the functionality should be installed on a separate host. Generally, a PSQL server for a specific funcionality should be on the same host as the dCache cell accessing that PSQL server, and the PSQL server containing the databases for CHIMERA should run on the same host as CHIMERA and the PnfsManager cell of the dCache system accessing it.

It is especially useful to use a separate PostgreSQL server for the `billing` cell.

> **Note**
>
> The following is *work-in-progress*.

Create PostgreSQL user with the name you will be using to run PNFS server. Make sure it has `CREATEDB` privilege.

```console-user
psql -U postgres template1 -c "CREATE USER johndoe with CREATEDB"
dropuser pnfsserver
createuser --no-adduser --createdb --pwprompt pnfsserver
```

Table 24.1. Protocol Overview

| Component        | Database Host                                                                | Database Name           | Database User | Database Password |
|------------------|------------------------------------------------------------------------------|-------------------------|---------------|-------------------|
| SRM              | `srm.db.host`or if not set: `srmDbHost` or if not set: localhost             | srm                     | dcache        | `--free--`        |
| PinManager       | `pinManagerDatabaseHost` or if not set: `srmDbHost` or if not set: localhost | pinmanager              | pinmanager    | `--free--`        |
| CELL-REPLICAMNGR | `replica.db.host` or if not set: localhost                                   | replica                 | dcache        | `--free--`        |
| PNFS server      | localhost                                                                    | admin, data1, exp0, ... | pnfsserver    | --free--          |
| billing          | `billingDatabaseHost` or if not set: localhost                               | billing                 | dcache        | `--free--`        |


