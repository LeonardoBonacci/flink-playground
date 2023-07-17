/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package guru.bonacci.flink;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.util.OutputTag;

import guru.bonacci.flink.connector.Sinks;
import guru.bonacci.flink.connector.Sources;
import guru.bonacci.flink.domain.Transfer;
import guru.bonacci.flink.domain.TransferErrors;
import guru.bonacci.flink.domain.TransferRule;
import guru.bonacci.flink.source.RuleGenerator;
import guru.bonacci.flink.source.TransferGenerator;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);
		
		final OutputTag<Transfer> outputTagValid = new OutputTag<Transfer>("valid"){};
		final OutputTag<Tuple2<Transfer, TransferErrors>> outputTagInvalid = new OutputTag<Tuple2<Transfer, TransferErrors>>("invalid"){};

		env.fromSource(Sources.createInputMessageConsumer("foo", "my-group"), WatermarkStrategy.noWatermarks(), "Kafka Source")
			.print();
		
		DataStream<TransferRule> ruleStream = env.addSource(new RuleGenerator())
				.map(rule -> {
						System.out.println(rule);
						return rule;
					}
				);
				
		DataStream<Transfer> transferRequestStream = env.addSource(new TransferGenerator())
			.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
			.filter(tf -> !tf.getFrom().equals(tf.getTo()));

		DataStream<Tuple2<Transfer, Boolean>> throttlingStream = transferRequestStream
			.keyBy(Transfer::getFrom)	
			.flatMap(new RequestThrottler()).name("throttle");
		
		SingleOutputStreamOperator<Transfer> mainThrottlingStream = throttlingStream
			  .process(new Splitter(outputTagValid, outputTagInvalid, TransferErrors.TOO_MANY_REQUESTS));

		DataStream<Transfer> throttledStream = mainThrottlingStream.getSideOutput(outputTagValid);
		
		mainThrottlingStream.getSideOutput(outputTagInvalid) // error handling
		 .map(tuple -> tuple.f1.toString() + ">" + tuple.f0.toString())
		 .sinkTo(Sinks.kafkaStringProducer("houston"));

		@SuppressWarnings("unchecked")
		AsyncRetryStrategy<Tuple2<Transfer, String>> asyncRetryStrategy =
			new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<Tuple2<Transfer, String>>(3, 100L) // maxAttempts=3, fixedDelay=100ms
				.ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
				.ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
				.build();
		
		 MapStateDescriptor<String, TransferRule> ruleStateDescriptor = 
		      new MapStateDescriptor<>(
		          "RulesBroadcastState",
		          BasicTypeInfo.STRING_TYPE_INFO,
		          TypeInformation.of(new TypeHint<TransferRule>() {}));
		 
		 BroadcastStream<TransferRule> ruleBroadcastStream = ruleStream.broadcast(ruleStateDescriptor);
		 
		 SingleOutputStreamOperator<Transfer> mainBalancedStream = 
				AsyncDataStream.orderedWaitWithRetry(throttledStream, new PostgresSufficientFunds(), 1000, TimeUnit.MILLISECONDS, 100, asyncRetryStrategy)
				.name("sufficient-funds")
				.connect(ruleBroadcastStream)
				.process(new DynamicBalanceSplitter(outputTagValid, outputTagInvalid));

		 mainBalancedStream.getSideOutput(outputTagInvalid) // error handling
			 .map(tuple -> tuple.f1.toString() + ">" + tuple.f0.toString())
			 .sinkTo(Sinks.kafkaStringProducer("houston"));

		 DataStream<Tuple2<Transfer, String>> lastRequestStream = mainBalancedStream.getSideOutput(outputTagValid)
				.keyBy(Transfer::getFrom)	
				.process(new LastRequestCache()).name("request-cache");

		 SingleOutputStreamOperator<Transfer> mainDbInSyncStream = AsyncDataStream.orderedWait(lastRequestStream, new PostgresLastRequestProcessed(), 1000, TimeUnit.MILLISECONDS, 1000)
					.name("db-in-sync")
				  .process(new Splitter(outputTagValid, outputTagInvalid, TransferErrors.TRANSFER_IN_PROGRESS));

		 mainDbInSyncStream.getSideOutput(outputTagInvalid) // error handling
			 .map(tuple -> tuple.f1.toString() + ">" + tuple.f0.toString())
			 .sinkTo(Sinks.kafkaStringProducer("houston"));

		 DataStream<Transfer> dbInSyncStream = mainDbInSyncStream.getSideOutput(outputTagValid);
		 dbInSyncStream.map(Transfer::toString).sinkTo(Sinks.kafkaStringProducer("foo"));
		 dbInSyncStream.addSink(
        JdbcSink.sink(
            "insert into transfers (id, _from, _to, pool_id, amount, timestamp) values (?, ?, ?, ?, ?, ?)",
            (statement, tf) -> {
                statement.setString(1, tf.getId().toString());
                statement.setString(2, tf.getFrom());
                statement.setString(3, tf.getTo());
                statement.setString(4, tf.getPoolId());
                statement.setDouble(5, tf.getAmount());
                statement.setLong(6, System.currentTimeMillis());
            },
            JdbcExecutionOptions.builder()
                    .withBatchSize(1000)
                    .withBatchIntervalMs(200)
                    .withMaxRetries(5)
                    .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:postgresql://localhost:5432/postgres")
                    .withDriverName("org.postgresql.Driver")
                    .withUsername("baeldung")
                    .withPassword("baeldung")
                    .build()
    ));
		
		env.execute("TES transfer-request processing");
	}
}
