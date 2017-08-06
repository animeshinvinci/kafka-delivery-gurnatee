package io.kafka.learn

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.internals.Topic
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._



class KafkaPublisherExactlyOnce {

  val producer = getProducer


  private def send(topic: String): Unit ={
    producer.initTransactions()
    import org.apache.kafka.clients.producer.ProducerRecord
    import org.apache.kafka.common.KafkaException
    import org.apache.kafka.common.errors.AuthorizationException
    import org.apache.kafka.common.errors.OutOfOrderSequenceException
    import org.apache.kafka.common.errors.ProducerFencedException

    try {
           producer.beginTransaction

           producer.send(new ProducerRecord[String, String](topic, Integer.toString(100), Integer.toString(100)))

           producer.commitTransaction
    } catch {
      case e@(_: ProducerFencedException | _: OutOfOrderSequenceException | _: AuthorizationException) =>
        // We can't recover from these exceptions, so our only option is to close the producer and exit.
        producer.close
      case e: KafkaException =>
        // For all other exceptions, just abort the transaction and try again.
        producer.abortTransaction
    }


  }

  private def getProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("linger.ms", "1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("enable.idempotence", "true")
    props.put("transactional.id", "my-fancy-id")

    new KafkaProducer[String, String](props)
  }


}

object KafkaPublisherExactlyOnce {

  def apply: KafkaPublisherExactlyOnce = new KafkaPublisherExactlyOnce()
}