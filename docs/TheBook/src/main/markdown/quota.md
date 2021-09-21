User and Group quota in dCache
==============================

Release 7.2 introduces a user and group quota implementation. The quota system operates on the chimera namespace. It periodically counts space usage broken down by Retention Policy, UID and GID, and stores these counts in two chimera DB tables –– t_user_quota and t_group_quota. Besides space counts these tables hold quota limits for each space count category by UID and by GID. A null for a limit means "no quota". No entries in these tables for a UID or GIDs means "no quota" for that UID or GID. CUSTODIAL Retention Policy corresponds to files going to tape, REPLICA corresponds to disk-only files, and OUTPUT currently is not used in dCache.  The quota system is controlled by the master switch:

```
        dcache.enable.quota
```

which by default is false. To enable the quota system, this variable has to be set to true on hosts running  the ```PnfsManager``` and ```NFS``` services.

Queries executed by periodic updates may run for a significant amount of time on the chimera back-end. Therefore default update frequency is once a day:

```
	pnfsmanager.quota.update.interval=1
	(one-of?MILLISECONDS|SECONDS|MINUTES|HOURS|DAYS)pnfsmanager.quota.update.interval.time.unit = DAYS
```

The default has been chosen to match an installation having close to 1B chimera file entries. On smaller installations it is advisable to increase the frequency by decreasing the above interval value.

This means that quota enforcement is eventual. If users run out of allocated quota they still will be able to write data over quota until the next update happens. Likewise, removing over quota data will not be noticed by the quota system until the next update.

Internally the quota check involves holding two maps `<uid, Quota>` (user quota map) and `<gid, Quota>` (group quota map) in memory at the level of `JdbcFs`. When create entry function is called the uid, gid and Retention Policy (based on RetentionPolicy tag of the parent directory or default retention policy if there is no tag) are used to check if maps have necessary entries and exception is thrown if space counts  exceed limits. Both maps are refreshed in memory from DB back-end every minute to pick possible new entries and new limits as well as to eventually catch the updated space usage counts.

From above follows that quota system is not going to work well with explicit
space reservations (or space reservations expressed as WriteToken tag) on dCache installations having both disk-only and tape-backed pools.
It requires RetentionPolicy tag. Although, since a general practice is to not to mix files with different retention policies under the same directories, this could work if RetentionPolicy tag is added to the containing directories.

On tape-only or disk-only installations, where Retention Policy of files is
unambiguous, the quota can rely on default, system-wide Retention policy value. On tape-only system, the default value supplied with dCache distribution is good:
```
    (one-of?CUSTODIAL|REPLICA|OUTPUT)dcache.default-retention-policy=CUSTODIAL
```
On disk-only systems, it needs to be set to :
```
    dcache.default-retention-policy=REPLICA
```

On a mixed system, a `dcache.default-retention-policy=CUSTODIAL` can be utilized for files bound to tape and RetentionPolicy tag can be set in the root of the directrory tree
where disk-only files are stored.

An admin user can interact with the quota system via the ```PnfsManager``` admin interface. For this, the following commands are provided:

```
	set group quota [OPTIONS] <gid>  # Set group quota
	set user quota [OPTIONS] <uid>  # Set user quota

	show group quota [-gid=<string>] [-h]  # Print group quota
	show user quota [-h] [-uid=<string>]  # Print user quota

```
Use ```help <command>``` to learn how to use the command.

A user-facing quota interface is implemented through RESTFul frontend.

A RESTful API for the quota system (see further below) has been implemented; it allows for creating (POST), modifying (PATCH), listing (GET) and removing (DELETE) both user and group quotas.

All commands except GET require admin privileges.  For GET, anonymous users are blocked.  Otherwise, a full list of quotas can be obtained using:

`curl   -X  GET  -H "Accept: application/json" http://localhost:3880/api/v1/quota/user `

`curl   -X  GET  -H "Accept: application/json" http://localhost:3880/api/v1/quota/group `

The list can be limited to the quota for the user's uid or primary gid by appending the query '\?user=true', or, if known, a GET can be issued with the id as final path element:

`curl   -X  GET  -H "Accept: application/json" http://localhost:3880/api/v1/quota/user/4215 `

etc.

A full description of the API is available through the Swagger interface (https://localhost:3880/api/v1).