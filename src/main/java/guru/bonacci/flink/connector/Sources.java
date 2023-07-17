package guru.bonacci.flink.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class Sources {

	 public static KafkaSource<String> createInputMessageConsumer(String topic, String kafkaGroup) {
		 return KafkaSource.<String>builder()
			    .setBootstrapServers("localhost:9092")
			    .setTopics(topic)
			    .setGroupId(kafkaGroup)
			    .setStartingOffsets(OffsetsInitializer.latest())
			    .setValueOnlyDeserializer(new SimpleStringSchema())
			    .build();
 }
}
