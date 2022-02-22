Example Configuration For needrestart
======================================
[needrestart](https://github.com/liske/needrestart) is a program that detects
which services need to be restarted after upgrades have been made.

Depending on it’s configuration it selects any such services for being
restarted.

In a production dCache-cluster this is however typically undesired.

The files `exclude-dcache.conf`, `exclude-postgresql.conf` and
`exclude-zookeeper.conf` serve as examples how needrestart can be configured to
not automatically select dCache, PostgreSQL, respectively ZooKeeper for being
restarted.
They need to be placed into `/etc/needrestart/conf.d/`.
