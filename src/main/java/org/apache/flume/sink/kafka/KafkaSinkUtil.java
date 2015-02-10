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

import java.util.Properties;

import com.google.common.collect.ImmutableMap;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaSinkUtil {
	private static final Logger log = LoggerFactory.getLogger(KafkaSinkUtil.class);
	private static final String serializedClazz = "serializer.class";

	public static Properties getKafkaConfigProperties(Context context) {
		log.info("context={}", context.toString());
		Properties props = new Properties();
		ImmutableMap<String, String> contextMap = context.getParameters();
		props.setProperty(serializedClazz,"kafka.serializer.StringEncoder");
		for (String key : contextMap.keySet()) {
			if (!key.equals("type") && !key.equals("channel")) {
				props.setProperty(key, context.getString(key));
				log.info("key={},value={}", key, context.getString(key));
			}
		}
		return props;
	}
}


















