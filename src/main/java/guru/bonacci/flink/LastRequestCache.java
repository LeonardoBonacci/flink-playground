package guru.bonacci.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

class LastRequestCache extends KeyedProcessFunction<String, Transfer, TransferStringWrapper> {

		private static final long serialVersionUID = 1L;

		private transient ValueState<String> transferIdState;

		@Override
		public void open(Configuration parameters) {
			ValueStateDescriptor<String> transferIdDescriptor = new ValueStateDescriptor<>(
					"transferId",
					Types.STRING);
			transferIdState = getRuntimeContext().getState(transferIdDescriptor);
		}

		@Override
		public void processElement(
				Transfer tf,
				Context context,
				Collector<TransferStringWrapper> collector) throws Exception {

			// Get the current state for the current key
			String lastTransferId = transferIdState.value();
			collector.collect(new TransferStringWrapper(tf, lastTransferId));

			transferIdState.update(tf.getId().toString());
		}
}