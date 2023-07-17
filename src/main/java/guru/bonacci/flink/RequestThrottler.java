package guru.bonacci.flink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import guru.bonacci.flink.domain.Transfer;

class RequestThrottler extends RichFlatMapFunction<Transfer, Tuple2<Transfer, Boolean>> {

  private static final long serialVersionUID = 1L;

  private transient ValueState<String> recentRequests;

  @Override
  public void flatMap(Transfer tf, Collector<Tuple2<Transfer, Boolean>> out) throws Exception {

      String previousRequestWithinLastMinute = recentRequests.value();
      
      if (previousRequestWithinLastMinute == null) {
      	out.collect(Tuple2.of(tf, true));
        recentRequests.update(tf.getId().toString());
      } else {
      	out.collect(Tuple2.of(tf, false));
      }
  }

  @Override
  public void open(Configuration config) {
  	StateTtlConfig ttlConfig = StateTtlConfig
  	    .newBuilder(Time.seconds(20)) 
  	    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
  	    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
  	    .build();
  	    
  	ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("request state", String.class);
  	stateDescriptor.enableTimeToLive(ttlConfig);

  	recentRequests = getRuntimeContext().getState(stateDescriptor);
  }
}