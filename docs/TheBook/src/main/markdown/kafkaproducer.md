
Apache-Kafka  with to dCache, or how to push events outside of dCache.
=======================




Starting from version 4.1 we introduced a new approach to external messaging. With the introduction of Apache-Kafka  (kafka.apache.org/documentation/) as a message transport, for now in Billing, we start switching to a more modern and efficient messaging system. The goal for the Billing service is easier inclusion with systems like Elasticsearch: Instead of adding messages to a textual log and having an external component parse that log, a Kafka-aware ingester can import them directly into the remote system.



What do you need to download to enable Kafka in dCache?
=======================


* ZooKeeper Framework

* Apache Kafka

Apache Kafka Installation.
=======================

1. Download
------------


To install Kafka on your machine, click on the below link −
https://www.apache.org/dyn/closer.cgi?path=/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz


Extract the tar file

    cd opt/
    tar -zxf kafka_2.11.0.9.0.0 tar.gz
    cd kafka_2.11.0.9.0.0


2. Enable kafka in dCache.
------------


    (one-of?true|false)dcache.enable.kafka = true


Set the broker address, or list of broker addresses

    (one-of?true|false)dcache.enable.kafka = true
    dcache.kafka.bootstrap-servers = localhost:9092






3. Start Server
------------



    bin/kafka-server-start.sh config/server.properties




4. Start consumer



    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic billing --from-beginning





