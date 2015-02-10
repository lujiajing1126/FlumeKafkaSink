/*******************************************************************************
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
 *******************************************************************************/
package org.apache.flume.sink.kafka;

import com.google.common.base.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import kafka.producer.ProducerConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

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
public class KafkaSink extends AbstractSink implements Configurable {
	private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);
	private String topic;
	private Producer<String, String> producer;
	private Context context;
	private Properties parameters;


	public Status process() throws EventDeliveryException {
		Status status = null;
		Channel channel = getChannel();
		Transaction tx = channel.getTransaction();
		tx.begin();
		try {
			Event event = channel.take();
			if (event == null) {
				tx.commit();
				return Status.READY;
			}
			String partitionKey = (String) parameters.get(KafkaFlumeConstants.PARTITION_KEY_NAME);
			String encoding = StringUtils.defaultIfEmpty((String) this.parameters.get(KafkaFlumeConstants.ENCODING_KEY_NAME),KafkaFlumeConstants.DEFAULT_ENCODING);
			String topic = Preconditions.checkNotNull((String) this.parameters.get(KafkaFlumeConstants.CUSTOME_TOPIC_KEY_NAME),"custom.topic.name is required");
			String eventData = new String(event.getBody(),encoding);
			KeyedMessage<String,String> data;

			if(StringUtils.isEmpty(partitionKey)) {
				data = new KeyedMessage<String, String>(topic,eventData);
			} else {
				data = new KeyedMessage<String, String>(topic,partitionKey,eventData);
			}

			producer.send(data);
			log.trace("Message: {}", event.getBody());
			tx.commit();
			status = Status.READY;
		} catch (Exception e) {
			tx.rollback();
			log.error("KafkaSink Exception:{}", e);
			status = Status.BACKOFF;
		} finally {
			tx.close();
		}
		return status;
	}

	public void configure(Context context) {
		this.context = context;
		parameters = KafkaSinkUtil.getKafkaConfigProperties(context);
		topic = parameters.getProperty(KafkaFlumeConstants.CUSTOME_TOPIC_KEY_NAME);
		if (topic == null) {
			throw new ConfigurationException("Kafka topic must be specified.");
		}
	}

	@Override
	public synchronized void start() {
		super.start();
		ProducerConfig config = new ProducerConfig(this.parameters);
		this.producer = new Producer<String, String>(config);
	}

	@Override
	public synchronized void stop() {
		producer.close();
		super.stop();
	}
}
