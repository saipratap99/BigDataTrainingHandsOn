import java.util.Properties
import java.util
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

object KafkaCustomPartitioner {


  def main(args: Array[String]) = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "UNIQUE-PRODUCER_ID")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("linger.ms", "1")
    props.put("batch.size","445")
    props.put("request.required.acks", "-1")
    props.put("partitioner.class", "KafkaUserCustomPartitioner");

    val producer = new KafkaProducer[String, String](props)

    val data = new ProducerRecord[String, String]("coda","Cassandra", "Distributed DB")

    val oncallback = new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception) = {
        println("Offset - " + recordMetadata.offset())
        println("Topic - " + recordMetadata.topic())
        println("Parition - " + recordMetadata.partition())
      }
    }

    try {
      producer.send(data, oncallback)

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

class KafkaUserCustomPartitioner extends Partitioner {

  override def close(): Unit = {}

  override def configure(map: util.Map[String, _]): Unit = {}

  override def partition(topic: String, key: scala.Any, keybytes: Array[Byte],
                         value: scala.Any, valuebytes: Array[Byte], cluster: Cluster): Int = {

    val key_ = key.asInstanceOf[String]
    if (key_.startsWith("M"))
      0
    else if (key_.startsWith("C"))
      1
    else
      2
  }
}