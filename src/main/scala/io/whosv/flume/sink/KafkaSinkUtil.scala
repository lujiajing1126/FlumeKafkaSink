package io.whosv.flume.sink

import java.util.Properties

import org.slf4j.{LoggerFactory, Logger}
import org.apache.flume.Context
import com.google.common.collect.ImmutableMap

/**
 * Created by megrez on 15/2/17.
 */
object KafkaSinkUtil {
  private val log: Logger = LoggerFactory.getLogger(classOf[KafkaSinkUtil])
  private val serializedClazz = "serializer.class"

  def get(context: Context): Properties = {
    log.info("context={}", context.toString)
    val props: Properties = new Properties
    val contextMap: ImmutableMap[String, String] = context.getParameters
    props.setProperty(serializedClazz, "kafka.serializer.StringEncoder")
    import scala.collection.JavaConversions._
    for (key <- contextMap.keySet) {
      if (!(key == "type") && !(key == "channel")) {
        props.setProperty(key, context.getString(key))
        log.info("key={},value={}", key, context.getString(key))
      }
    }
    props
  }
}

class KafkaSinkUtil {
  def getKafkaConfigProperties(context: Context): Properties = KafkaSinkUtil.get(context: Context): Properties
}


