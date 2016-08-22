package kafka.consumer

import scala.collections.JavaConversions._
import kafka.message._
import kafka.utils._
import java.util.Properties
import kafka.utils.Logging

class kafkaConsumer(
	topic: String,
	groupId: String,
	zookeeperconnect: String,
	readFromStartOfStream: Boolean = true
	) extends logging {

	val props = new Properties()
	props.put("group.id", groupId)
	props.put("zookeeper.connect", zookeeperconnect)
	props.put("auto.offset.reset", if(readFromStartOfStream) "smallest" else "largest")

	val config = new ConsumerConfig(props)
	val connector = Consumer.create(config)
	val filter = new WhiteList(topic)


//setup start
	val stream = connector.createMessageStreamByFilter(filter, 1, new DefaultDecoder(), new DefaultDecoder.get(0))
//setup complete

	def read(w: (Array[Byte])=>Unit) = {
//read from stream
		for (messageAndTopic <- stream) {
			try {
				write(messageAndTopic.message)
			}

			catch{
				case e: Throwable =>
					if (true) error("Error processing message", e)
					else throw e

			}


		}


	}

	def close(){
		connector.shutdown()
	}


}


