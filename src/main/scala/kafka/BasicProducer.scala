package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object BasicProducer {

  def main(args: Array[String]): Unit = {
    print("Running Producer")
    if (args.length == 2)
      writeToKafka(args(0), args(1))
    else
      throw new IllegalStateException("Arguments topic and broker nedded")
  }

  def writeToKafka(topic: String, brokers: String = "localhost:9092"): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    for {i <- 1 to 10} {
      val record = new ProducerRecord[String, String](topic, "key", i.toString)
      producer.send(record)
    }
    producer.close()
  }
}
