package guru.bonacci.flink;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import guru.bonacci.flink.domain.Transfer;
import guru.bonacci.flink.domain.TransferRule;
import guru.bonacci.flink.domain.TransferStringWrapper;

public class DynamicBalanceRuler extends BroadcastProcessFunction<TransferStringWrapper, TransferRule, Transfer> {

  private final MapStateDescriptor<String, TransferRule> ruleStateDescriptor = 
      new MapStateDescriptor<>(
          "RulesBroadcastState",
          BasicTypeInfo.STRING_TYPE_INFO,
          TypeInformation.of(new TypeHint<TransferRule>() {}));
  
	@Override
	public void processBroadcastElement(TransferRule rule, Context ctx, Collector<Transfer> out) throws Exception {
		ctx.getBroadcastState(ruleStateDescriptor).put(rule.getPoolType(), rule);
	}

	@Override
	public void processElement(TransferStringWrapper tfWrapper, ReadOnlyContext ctx, Collector<Transfer> out) throws Exception {
		ReadOnlyBroadcastState<String, TransferRule> rulesState = ctx.getBroadcastState(ruleStateDescriptor);
		TransferRule poolTypeRule = rulesState.get(tfWrapper.getTransfer().getPoolType());

		if (Double.valueOf(tfWrapper.getStr()) > poolTypeRule.getMinBalance()) {
			out.collect(tfWrapper.getTransfer());
		}
	}
}
