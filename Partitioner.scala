package Kafka.producer

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

class simplePartioner extends Partitioner{
  def simplepartitioner (props: VerifiableProperties): Null= Null
  
  def partition(key: Any, npart: Int): Int = {
    val partition = 0
    val stringKey = key.toString;
    val offset = stringKey.lastIndexOf('.')
    if (offset >0) {
      partition = Integer.parseInt(stringKey.substring(offset+1))% npart
    }
    else partition = 0
    
    return partition
    
  }
  
}
