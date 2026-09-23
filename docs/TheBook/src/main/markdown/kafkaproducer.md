
Apache-Kafka  with dCache, or how to push events outside of dCache.
=======================

Starting from version 4.1 we introduced a new approach to external messaging. With the introduction of Apache-Kafka  (kafka.apache.org/documentation/) as a message transport, for now in Billing, we start switching to a more modern and efficient messaging system. The goal for the Billing service is easier inclusion with systems like Elasticsearch: Instead of adding messages to a textual log and having an external component parse that log, a Kafka-aware ingester can import them directly into the remote system.

-----
[TOC bullet hierarchy]
-----

## What do you need to download to enable Kafka in dCache?


* ZooKeeper Framework

* Apache Kafka

## Apache Kafka Installation.

### 1. Download


To install Kafka on your machine, click on the below link −
https://www.apache.org/dyn/closer.cgi?path=/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz


Extract the tar file

```console-root
cd opt/
tar -zxf kafka_2.11.0.9.0.0 tar.gz
cd kafka_2.11.0.9.0.0
```

### 2. Enable kafka in dCache.


```ini
dcache.enable.kafka = true
```

Set the broker address, or list of broker addresses

```ini
dcache.kafka.bootstrap-servers = localhost:9092
```

Set kafka topic name

```ini
dcache.kafka.topic = billing
```

"billing" is default. The following service level variables
reference dcache.kafka.topic:

   ftp.kafka.topic = ${dcache.kafka.topic}
   pool.kafka.topic = ${dcache.kafka.topic}
   webdav.kafka.topic = ${dcache.kafka.topic}
   xrootd.kafka.topic = ${dcache.kafka.topic}

> **Note**: Kafka producer support has been removed from the NFS door. The `nfs.kafka.*` properties are obsolete and no longer have any effect.

### 3. Start Server

```console-root
bin/kafka-server-start.sh config/server.properties
```

### 4. Start consumer


```console-root
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic billing --from-beginning
```
