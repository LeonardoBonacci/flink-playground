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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;

import guru.bonacci.flink.domain.Transfer;
import guru.bonacci.flink.domain.TransferRule;
import guru.bonacci.flink.domain.TransferStringWrapper;
import guru.bonacci.flink.domain.TransferValidityWrapper;
import guru.bonacci.flink.source.RuleGenerator;
import guru.bonacci.flink.source.TransferGenerator;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);
		
		DataStream<TransferRule> ruleStream = env.addSource(new RuleGenerator())
				.map(new MapFunction<TransferRule, TransferRule>() {

					@Override
					public TransferRule map(TransferRule value) throws Exception {
						System.out.println(value);
						return value;
					}
				});
				
		DataStream<Transfer> transferRequestStream = env.addSource(new TransferGenerator())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Transfer>() {
				@Override
				public long extractAscendingTimestamp(Transfer tf) {
					return tf.getTimestamp();
				}
			})
			.filter(new FilterFunction<Transfer>() {
				
				@Override
				public boolean filter(Transfer tf) throws Exception {
					return !tf.getFrom().equals(tf.getTo());
				}
			});

		DataStream<Transfer> throttledStream = transferRequestStream
			.keyBy(Transfer::getFrom)	
			.flatMap(new RequestThrottler()).name("throttle")
			.filter(new FilterFunction<TransferValidityWrapper>() {
				
				@Override
				public boolean filter(TransferValidityWrapper wrapper) throws Exception {
					return wrapper.isValid();
				}
			})
			.map(new MapFunction<TransferValidityWrapper, Transfer>() {

				@Override
				public Transfer map(TransferValidityWrapper wrapper) throws Exception {
					return wrapper.getTransfer();
				}
			});

		@SuppressWarnings("unchecked")
		AsyncRetryStrategy<TransferStringWrapper> asyncRetryStrategy =
			new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<TransferStringWrapper>(3, 100L) // maxAttempts=3, fixedDelay=100ms
				.ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
				.ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
				.build();
		
		 MapStateDescriptor<String, TransferRule> ruleStateDescriptor = 
		      new MapStateDescriptor<>(
		          "RulesBroadcastState",
		          BasicTypeInfo.STRING_TYPE_INFO,
		          TypeInformation.of(new TypeHint<TransferRule>() {}));
		 
		 BroadcastStream<TransferRule> ruleBroadcastStream = ruleStream
         .broadcast(ruleStateDescriptor);
		 
		DataStream<Transfer> balancedStream = 
				AsyncDataStream.orderedWaitWithRetry(throttledStream, new PostgresSufficientFunds(), 1000, TimeUnit.MILLISECONDS, 100, asyncRetryStrategy)
				.name("sufficient-funds")
				.connect(ruleBroadcastStream)
				.process(new DynamicBalanceRuler());

		DataStream<TransferStringWrapper> lastRequestStream = balancedStream
				.keyBy(Transfer::getFrom)	
				.process(new LastRequestCache()).name("request-cache");

			DataStream<Transfer> dbInSyncStream = AsyncDataStream.orderedWait(lastRequestStream, new PostgresLastRequestProcessed(), 1000, TimeUnit.MILLISECONDS, 1000)
				.name("db-in-sync")
				.filter(new FilterFunction<TransferValidityWrapper>() {
					
					@Override
					public boolean filter(TransferValidityWrapper wrapper) throws Exception {
						return wrapper.isValid();
					}
				})
				.map(new MapFunction<TransferValidityWrapper, Transfer>() {
		
					@Override
					public Transfer map(TransferValidityWrapper wrapper) throws Exception {
						return wrapper.getTransfer();
					}
				});
		
			dbInSyncStream.print();
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
