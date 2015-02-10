## FlumeKafkaSink

This project is a sink plugin for [flume-ng](http://flume.apache.org/) producing to [kafka 0.8.2.0 with Scala 2.10](http://kafka.apache.org/).

### Install

```
// Assembly with dependencies
mvn assembly:assembly -DskipTests=true
```

### Config for Flume-NG

```
# example.conf: A single-node Flume configuration

# Name the components on this agent
a1.sources = r1
a1.sinks = k1 k2
a1.channels = c1 c2

# Describe/configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = 192.168.2.102
a1.sources.r1.port = 44444

a1.channels.c1.type = memory
a1.channels.c2.type = memory
# Bind the source and sink to the channel
a1.sources.r1.channels = c1 c2
a1.sinks.k2.channel = c2

a1.sinks.k2.type = logger
a1.sinks.k1.channel = c1
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.metadata.broker.list = 127.0.0.1:9092
a1.sinks.k1.serializer.class = kafka.serializer.StringEncoder
a1.sinks.k1.request.required.acks = 0
a1.sinks.k1.producer.type = sync
a1.sinks.k1.custom.encoding = UTF-8
a1.sinks.k1.custom.topic.name = kafkaToptic
```

###

You can test the result by running the ```KafkaConsumer``` in the test directory.

### Acknowledge

[beyondj2ee/flumeng-kafka-plugin](https://github.com/beyondj2ee/flumeng-kafka-plugin)

[baniuyao/flume-ng-kafka-sink](https://github.com/baniuyao/flume-ng-kafka-sink)

