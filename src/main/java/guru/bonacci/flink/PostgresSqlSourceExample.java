package guru.bonacci.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class PostgresSqlSourceExample {
	
  public static void main(String[] args) throws Exception {

  	 SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
  	      .hostname("localhost")
  	      .port(5432)
  	      .database("postgres")
          .tableList("public.transfers") 
          .username("baeldung")
          .password("baeldung")
          .decodingPluginName("pgoutput")
  	      .deserializer(new JsonDebeziumDeserializationSchema())
  	      .build();

  	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  	    env
  	      .addSource(sourceFunction)
  	      .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

  	    env.execute();
  } 
}