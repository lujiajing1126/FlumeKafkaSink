/** *****************************************************************************
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  * ******************************************************************************/
package io.whosv.flume.sink.kafka

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
class KafkaSink extends AbstractSink with Configurable {
  private val log: Logger = LoggerFactory.getLogger(classOf[KafkaSink])

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
