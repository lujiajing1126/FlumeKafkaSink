package io.whosv.flume.sink

import com.google.common.base.Preconditions
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.apache.commons.lang.StringUtils
import org.apache.flume._
import org.apache.flume.Sink.Status
import org.apache.flume.conf.Configurable
import org.apache.flume.conf.ConfigurationException
import org.apache.flume.sink.AbstractSink
import io.whosv.flume.sink.{KafkaSinkUtil, KafkaFlumeConstants}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * A Sink of Kafka which get events from channels and publish to Kafka. I use
 * this in our company production environment which can hit 100k messages per
 * second. <tt>zk.connect: </tt> the zookeeper ip kafka use.
 * <p>
 * <tt>topic: </tt> the topic to read from kafka.
 * <p>
 * <tt>batchSize: </tt> send serveral messages in one request to kafka.
 * <p>
 * <tt>producer.type: </tt> type of producer of kafka, async or sync is
 * available.<o> <tt>serializer.class: </tt>{@kafka.serializer.StringEncoder}
 *
 */
object KafkaSink {
  private val log: Logger = LoggerFactory.getLogger(classOf[KafkaSink])
}

class KafkaSink extends AbstractSink with Configurable {
  private var topic: String = null
  private var producer: Producer[String, String] = null
  private var context: Context = null
  private var parameters: Properties = null

  @throws(classOf[EventDeliveryException])
  def process: Sink.Status = {
    var status: Sink.Status = null
    val channel: Channel = getChannel
    val tx: Transaction = channel.getTransaction
    tx.begin
    try {
      val event: Event = channel.take
      if (event == null) {
        tx.commit
        return Status.READY
      }
      val partitionKey: String = parameters.get(KafkaFlumeConstants.PARTITION_KEY_NAME).asInstanceOf[String]
      val encoding: String = StringUtils.defaultIfEmpty(this.parameters.get(KafkaFlumeConstants.ENCODING_KEY_NAME).asInstanceOf[String], KafkaFlumeConstants.DEFAULT_ENCODING)
      val topic: String = Preconditions.checkNotNull(this.parameters.get(KafkaFlumeConstants.CUSTOME_TOPIC_KEY_NAME).asInstanceOf[String], "custom.topic.name is required")
      val eventData: String = new String(event.getBody, encoding)
      var data: KeyedMessage[String, String] = null
      if (StringUtils.isEmpty(partitionKey)) {
        data = new KeyedMessage[String, String](topic, eventData)
      }
      else {
        data = new KeyedMessage[String, String](topic, partitionKey, eventData)
      }
      producer.send(data)
      KafkaSink.log.trace("Message: {}", event.getBody)
      tx.commit
      status = Status.READY
    }
    catch {
      case e: Exception => {
        tx.rollback
        KafkaSink.log.error("KafkaSink Exception:{}", e)
        status = Status.BACKOFF
      }
    } finally {
      tx.close
    }
    return status
  }

  def configure(context: Context) {
    this.context = context
    parameters = KafkaSinkUtil.getKafkaConfigProperties(context)
    topic = parameters.getProperty(KafkaFlumeConstants.CUSTOME_TOPIC_KEY_NAME)
    if (topic == null) {
      throw new ConfigurationException("Kafka topic must be specified.")
    }
  }

  override def start {
    super.start
    val config: ProducerConfig = new ProducerConfig(this.parameters)
    this.producer = new Producer[String, String](config)
  }

  override def stop {
    producer.close
    super.stop
  }
}
