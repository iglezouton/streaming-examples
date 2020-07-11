package kafka

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import kafka.BasicProducer.writeToKafka

import scala.concurrent.duration
import scala.jdk.CollectionConverters._

object BasicConsumer {

  def main(args: Array[String]): Unit = {
    print("Running Consumer")
    if (args.length == 2)
      consumeFromKafka(args(0), args(1))
    else
      throw new IllegalStateException("Arguments topic and broker nedded")
  }

  def consumeFromKafka(topic: String, brokers: String = "localhost:9092") = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(Duration.ofSeconds(1)).asScala
      for (data <- record.iterator)
        println(data.value())
      Thread.sleep(1L)
    }
  }
}
