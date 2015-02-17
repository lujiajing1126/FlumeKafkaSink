package io.whosv.flume.sink

/**
 * Created by megrez on 15/2/17.
 */
object KafkaFlumeConstants {
  /**
   * The constant PARTITION_KEY_NAME.
   */
  val PARTITION_KEY_NAME = "custom.partition.key"
  /**
   * The constant ENCODING_KEY_NAME.
   */
  val ENCODING_KEY_NAME = "custom.encoding"
  /**
   * The constant DEFAULT_ENCODING.
   */
  val DEFAULT_ENCODING = "UTF-8"
  /**
   * The constant CUSTOME_TOPIC_KEY_NAME.
   */
  val CUSTOME_TOPIC_KEY_NAME = "custom.topic.name"
  /**
   * The constant CUSTOME_TOPIC_KEY_NAME.
   */
  val CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME = "custom.thread.per.consumer"
}
