package org.apache.flume.sink.kafka;

/**
 * Created by megrez on 15/2/10.
 */
public class KafkaFlumeConstants {
    /**
     * The constant PARTITION_KEY_NAME.
     */
    public static final String PARTITION_KEY_NAME = "custom.partition.key";
    /**
     * The constant ENCODING_KEY_NAME.
     */
    public static final String ENCODING_KEY_NAME = "custom.encoding";
    /**
     * The constant DEFAULT_ENCODING.
     */
    public static final String DEFAULT_ENCODING = "UTF-8";
    /**
     * The constant CUSTOME_TOPIC_KEY_NAME.
     */
    public static final String CUSTOME_TOPIC_KEY_NAME = "custom.topic.name";

    /**
     * The constant CUSTOME_TOPIC_KEY_NAME.
     */
    public static final String CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME = "custom.thread.per.consumer";
}
