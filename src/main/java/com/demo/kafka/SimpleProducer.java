package com.demo.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {

	private static Producer<Integer, String>	producer;
	private final Properties					props	= new Properties();

	public SimpleProducer() {
		//注意:broker配置文件server.properties中配置项advertised.host.name需要配置
		// Set the broker list for requesting metadata to find the lead broker
		props.put("metadata.broker.list", "10.110.25.196:9093,10.110.25.197:9093,10.110.25.198:9093");
		// props.put("metadata.broker.list", "test-213:9093, test-213:9094");
		//This specifies the serializer class for for the messages
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 1 means the producer receives an acknowledgment once the lead replica
		// has received the data. This option provides better durability as the
		// client waits until the server acknowledges the request as successful.
		props.put("request.required.acks", "1");
		producer = new Producer<Integer, String>(new ProducerConfig(props));
	}

	public static void main(String[] args) {
		SimpleProducer sp = new SimpleProducer();
		String topic = "mytopic";
		String messageStr = "helloworld";
		KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic, messageStr);
		producer.send(data);
		System.out.println("send");
		producer.close();
	}

}
