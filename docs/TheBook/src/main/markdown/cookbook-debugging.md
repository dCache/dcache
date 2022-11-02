# Debugging

This chapter provides an information on how to exam running dCache system, identify problems and provide debug information to developers.

## Java Flight recorder

When debugging an issue on a running system often we need to collect jvm performance stats with `Java flight recorder`. Starting from release 7.2 the Java flight recorder attach listener is enabled by default. Site admins can collect and provide developers with additional information when high CPU load, memory consumption or file descriptor leaks are observed. To enable the `flight recorder` _jcmd_ command is used, which is typically provided as a part of `java-11-openjdk-devel` (on RHEL and clones).

To control recoding the following subcommands of _jcmd_ available:

- JFR.start
  - Start a recording.

- JFR.stop
  - Stop a recording with a specific identification number.

- JFR.dump
  - Dump the data collected so far by the recording with a specific identification number.

> Example:

```
jcmd <pid> JFR.start filename=/tmp/dcache.jfr
```

The java process _pid_ can be obtained with _systemctl_ or _jps_ command.
There are several to limit the recording:

- time duration based
- Limiting by time window
- Limiting by dump file size

Typically, a duration or time window based recordings should be used.

### Duration based recording

Useful to record for a fixed period of time. The duration can be specified in in (s)econds, (m)inutes, (h)ours, or (d)ays:

```
jcmd <pid> JFR.start duration=60s filename=/tmp/dcache.jfr
```

The recording file will be written into defined file after specified time duration. The flight recorder will be automatically switched off.

### Time window based limit

The recording is collected in a ring-buffer like state with a fixed time-based limit. Such recording is useful in situations when we can't predict the point in time of the interested event.

```
jcmd <pid> JFR.start maxage=10m name=my-10m-records
```

The `name=my-10m-records` option allows to give a human readable identifier to the recording.

The recording can be collected as:

```
jcmd <pid> JFR.dump name=my-10m-records filename=/tmp/dcache.jfr
```

The flight recorder will stay active and new recording can be collected, if needed.

To stop the recording the following command can be used:

```
jcmd <pid> JFR.stop name=my-10m-records
```

### Size based limit

The recording can be started with an explicit maximum recorded data size. The size can be specified in in (k)B, (M)B or (G)B.

```
jcmd <pid> JFR.start maxsize=100K name=my-100k-records
```

### Inspecting flight recordings

Typically, the collected data should be provided to dCache development team. However, curious sysadmins can inspect the recordings with [Oracle VisualVM](https://github.com/oracle/visualvm/releases) or [Oracle JMC](https://www.oracle.com/java/technologies/jdk-mission-control.html)
