package io.whosv.flume.sink.kafka

/**
 * Created by megrez on 15/2/12.
 */
class KafkaFlumeConstants {
  /**
   * The constant PARTITION_KEY_NAME.
   */
  val PARTITION_KEY_NAME: String = "custom.partition.key"
  /**
   * The constant ENCODING_KEY_NAME.
   */
  val ENCODING_KEY_NAME: String = "custom.encoding"
  /**
   * The constant DEFAULT_ENCODING.
   */
  val DEFAULT_ENCODING: String = "UTF-8"
  /**
   * The constant CUSTOME_TOPIC_KEY_NAME.
   */
  val CUSTOME_TOPIC_KEY_NAME: String = "custom.topic.name"
  /**
   * The constant CUSTOME_TOPIC_KEY_NAME.
   */
  val CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME: String = "custom.thread.per.consumer"
}
