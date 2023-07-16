package guru.bonacci.flink.source;

import java.util.Map;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import com.google.common.collect.ImmutableMap;

import guru.bonacci.flink.domain.TransferRule;

public class RuleGenerator extends RichSourceFunction<TransferRule> {

	public final static String EASY = "easy";	
	public final static String MEDIUM = "medium";	
	public final static String HARD = "hard";	
		
	public final static Map<String, Double> pools = ImmutableMap.of(EASY, -40.00, MEDIUM, 0.00, HARD, 40.00);

	@Override
	public void run(SourceContext<TransferRule> sourceContext) throws Exception {
		pools.forEach((type, bal) -> sourceContext.collect(new TransferRule(type, bal)));
	}

	@Override
	public void cancel() {
	}
}