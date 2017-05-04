package com.demo.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {

	public SimplePartitioner(VerifiableProperties props) {

	}

/*
* The method takes the key, which in this case is the IP address,
* It finds the last octet and does a modulo operation on the number
* of partitions defined within Kafka for the topic.
*
* @see kafka.producer.Partitioner#partition(java.lang.Object, int)
*/
	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt(stringKey.substring(offset + 1)) % a_numPartitions;
		}
		return partition;
	}

}
