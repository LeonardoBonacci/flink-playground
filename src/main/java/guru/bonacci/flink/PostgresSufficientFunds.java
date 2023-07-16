package guru.bonacci.flink;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.val;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import com.github.jasync.sql.db.Connection;
import com.github.jasync.sql.db.QueryResult;
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder;

import guru.bonacci.flink.domain.Transfer;

class PostgresSufficientFunds extends RichAsyncFunction<Transfer, Tuple2<Transfer, String>> {

		private static final long serialVersionUID = 1L;

		private String CONNECTION_URL = "jdbc:postgresql://127.0.0.1:5432/postgres?user=baeldung&password=baeldung";
		private transient Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
    	conn = PostgreSQLConnectionBuilder.createConnectionPool(CONNECTION_URL);
    }

    @Override
    public void close() throws Exception {
    	conn.disconnect().get();
    }

    /**
     * select sum(amount) as balance
			 from (
					select -1 * sum (amount) as amount from transfers where _from = 'aaaa'
					union
					select sum (amount) from transfers where _to = 'a'
			) AS tmp;	
     */
    @Override
    public void asyncInvoke(Transfer tf, final ResultFuture<Tuple2<Transfer, String>> resultFuture) throws Exception {
    	String query = DSL.using(SQLDialect.POSTGRES)
      		.select(sum(field("amount").coerce(Double.class)).as("balance"))
  				.from(
  						select(
  								sum(field("amount").coerce(Double.class)).mul(val(-1)).as("amount"))
  						.from("transfers")
  						.where(field("_from").eq(tf.getFrom()))
  						.union(
  								select(
  										sum(field("amount").coerce(Double.class).as("amount")))
  								.from("transfers")
  								.where(field("_to").eq(tf.getFrom()))
							)
  				).getSQL(ParamType.INLINED);
    	
    	CompletableFuture<QueryResult> result = conn.sendPreparedStatement(query);
    	CompletableFuture.supplyAsync(new Supplier<QueryResult>() {

          @Override
          public QueryResult get() {
              try {
                  return result.get();
              } catch (InterruptedException | ExecutionException e) {
                  return null;
              }
          }
      }).thenAccept( (QueryResult queryResult) -> {
        resultFuture.complete(
        		Collections.singleton(
        				Tuple2.of(tf, queryResult.getRows().get(0).get(0).toString())));
      });
    }
}