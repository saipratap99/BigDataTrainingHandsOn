import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties;

object DataProducer {
  def main(args: Array[String]) = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "UNIQUE-PRODUCER_ID")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("linger.ms", "1")
    props.put("batch.size","445")
    props.put("request.required.acks", "-1")
    //props.put("partitioner.class", "com.dmac.KafkaUserCustomPartitioner");

    val producer = new KafkaProducer[String, String](props)

    val data = new ProducerRecord[String, String]("coda-global","zion", "kafka is ultimate messaging")

    try {
      producer.send(data)
    }
    catch {
      case ex: Exception =>{
        println(ex)
      }

    }


    // Kafka producer produces a flush method to ensure all previously sent messages have been actually completed.
    producer.flush()
    producer.close()
  }


}
