package main;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaSender {

	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static String TOPIC = "stock-topic";

	public static Producer<String, String> createProducer() {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	
	}

	public static void sendStockData(String index, String high)
			throws Exception {

		final Producer<String, String> producer = createProducer();
		final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
				TOPIC, index, high);
		System.out.println(producerRecord);
		producer.send(producerRecord).get();

	}
}
