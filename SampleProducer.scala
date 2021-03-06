package Kafka.producer

import java.util.{Properties, UUID}
import java.io._
import kafka.common._
import kafka.message._
import kafka.producer.Producer
import kafka.serializer._

case class KafkaProducer(

    topic: String,
    brokerList: String,
    clientId: String= UUID.randomUUID().toString,
    synchronously: Boolean =true,
    batchSize: Int = 200,
    messageSendMaxRetries: Int = 3,
    requestRequireAcks: Int = -1
   ) {
  
  val props = new Properties()
 
  //Properties from Java
  
  props.put("producer.type", if (synchronously) "sync" else "async")
  props.put("broker.list", brokerList)
  props.put("batch.num.messages", batchSize.toString)
  props.put("message.send.max.retries", messageSendMaxRetries.toString)
  props.put("request.require.acks", requestRequireAcks.toString)
  props.put("client.id", clientId.toString)
  props.put("serializer.class", "kafka.serializer.stringEncoder")
  


  val config = new ProducerConfig (props)
  //ProducerConfig: part of Kafka Producer
  val producer = new Producer[AnyRef, AnyRef](config)
  // Producer needs Key: Anyref (partition key type), Value :AnyRef (message) and config


  def kafkaMessage(message: Array[Byte], partition: Array[Byte]): KeyedMessage[AnyRef, AnyRef] = {
// KeyedMessage:  Part of kafka core
    if (partition== null) {
      new KeyedMessage(topic, message)
    }
    else {
      new KeyedMessage(topic, partition, message)
    }


  }
  def sendMessage(message: String, partition: String ): Unit = {
    try {
      producer.send(kafkaMessage(message, partition))
    }
    catch {
      case e : Exception =>
        e.printStackTrace
        System.exit(1)
    }
  }

}
