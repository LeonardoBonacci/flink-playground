package guru.bonacci.flink.connector;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import guru.bonacci.flink.domain.Transfer;

public class TransferDeserializationSchema implements KafkaRecordDeserializationSchema<Tuple2<Transfer, String>> {

	private static final long serialVersionUID = 1L;

	private final SimpleStringSchema stringSchema = new SimpleStringSchema();
	private final JsonDeserializationSchema<Transfer> valueDeserializer = new JsonDeserializationSchema<>(Transfer.class);

	 @Override
	 public void open(DeserializationSchema.InitializationContext context) throws Exception {
		 stringSchema.open(context);
		 valueDeserializer.open(context);
	 }
	
	@Override
	public TypeInformation<Tuple2<Transfer, String>> getProducedType() {
		return TypeInformation.of(new TypeHint<Tuple2<Transfer, String>>() {});
	}

	@Override
	public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Tuple2<Transfer, String>> out)
			throws IOException {
		String k = stringSchema.deserialize(record.key());
		Transfer v = valueDeserializer.deserialize(record.value());
		out.collect(Tuple2.of(v, "Hi " + k));
	}
}
