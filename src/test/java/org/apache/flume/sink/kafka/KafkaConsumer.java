package org.apache.flume.sink.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
    private CyclicBarrier cyclicBarrier;
    public KafkaConsumer(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, a_numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);
        cyclicBarrier = new CyclicBarrier(streams.size());
        // now create an object to consume the messages
        int threadNumber = 0;

        for (final KafkaStream<byte[],byte[]> stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber,cyclicBarrier));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("consumer.id","megrez-reset");
        return new ConsumerConfig(props);
    }

    public static void main(String[] args) {
        String zooKeeper = "127.0.0.1";
        String groupId = "1";
        String topic = "kafkaToptic";
        int threads = Integer.parseInt("1");

        KafkaConsumer example = new KafkaConsumer(zooKeeper, groupId, topic);
        example.run(threads);

        example.shutdown();
    }
}
