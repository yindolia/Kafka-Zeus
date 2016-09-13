package zeus.kafka.examples.streams

import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{Kstream, KstreamBuilder}


object MapFunctionEx {
	def main(args: Array[String]) {

		val builder: KstreamBuilder = new KstreamBuilder

		val streamingConfig = {
			settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-scala")
			settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
			settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
			//default serializers

			settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
			settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

		}

		val stringSerde: Sede[String]= Serdes.String()

		//Read input kafka topic into a Kstream instance.
		val textLines: KStream[Array[Byte], String] = builder.stream("TextLinesTopic")

		//Variant 1: using 'mapValues'

		val uppercasedWithMapValues: KStream[Array[Byte], String]= textLines.mapValues(_.toUpperCase())

		//Variant 2: using map and modify value only

		import KeyValueImplicits._

		val uppercasedWithMap: Kstream[Array[Byte], String]= textLines.map((key,value)=> (key, value.toUpperCase))
		


	}



}
