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
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);
		
		DataStream<Transfer> transferRequests = env.addSource(new TransferGenerator())
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

		DataStream<Transfer> throttled = transferRequests
			.keyBy(Transfer::getIdentifier)	
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

		DataStream<TransferStringWrapper> lastRequests = throttled
			.keyBy(Transfer::getIdentifier)	
			.process(new LastRequestCache()).name("request-cache");

		DataStream<Transfer> dbInSync = AsyncDataStream.orderedWait(lastRequests, new PostgresLastRequestProcessed(), 1000, TimeUnit.MILLISECONDS, 1000)
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

		@SuppressWarnings("unchecked")
		AsyncRetryStrategy<TransferValidityWrapper> asyncRetryStrategy =
			new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<TransferValidityWrapper>(3, 100L) // maxAttempts=3, fixedDelay=100ms
				.ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
				.ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
				.build();
		
		DataStream<Transfer> insertableTransferStream = 
				AsyncDataStream.orderedWaitWithRetry(dbInSync, new PostgresSufficientFunds(), 1000, TimeUnit.MILLISECONDS, 100, asyncRetryStrategy)
				.name("sufficient-funds")
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

		insertableTransferStream.print();
		insertableTransferStream.addSink(
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
