package guru.bonacci.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import guru.bonacci.flink.domain.Transfer;
import guru.bonacci.flink.domain.TransferErrors;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class Splitter extends ProcessFunction<Tuple2<Transfer, Boolean>, Transfer> {

	private static final long serialVersionUID = 1L;

	private final OutputTag<Transfer> outputTagValid;
	private final OutputTag<Tuple2<Transfer, TransferErrors>> outputTagInvalid;
	private final TransferErrors errorMessage;
	
  @Override
  public void processElement(
  		Tuple2<Transfer, Boolean> tuple,
      Context ctx,
      Collector<Transfer> out) throws Exception {

    if (tuple.f1) {
      ctx.output(outputTagValid, tuple.f0);
    } else {
      ctx.output(outputTagInvalid, Tuple2.of(tuple.f0, errorMessage));
    }
  }
}
