package guru.bonacci.flink;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.val;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.ResultSetGroup;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import guru.bonacci.flink.domain.Transfer;

class PinotSufficientFunds extends RichAsyncFunction<Transfer, Tuple2<Transfer, Double>> {

		private static final long serialVersionUID = 1L;

		private transient Connection pinotConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
       pinotConnection = ConnectionFactory.fromHostList("localhost:8099");
    }

    @Override
    public void close() throws Exception {
    	pinotConnection.close();
    }

    /**
     * select -1 * sum (amount) as amount from transfers where _from = 'aaaa'
		 * select sum (amount) from transfers where _to = 'a'
     */
    @Override
    public void asyncInvoke(Transfer tf, final ResultFuture<Tuple2<Transfer, Double>> resultFuture) throws Exception {
     	String creditQuery = DSL.using(SQLDialect.POSTGRES)
  				.select(
  						sum(field("amount").coerce(Double.class)).mul(val(-1)).as("amount"))
  				.from("transfers_4")
  				.where(field("fromId").eq(tf.getFromId()))
  				.getSQL(ParamType.INLINED);

     	String debitQuery = DSL.using(SQLDialect.POSTGRES)
  				.select(
  						sum(field("amount").coerce(Double.class)).as("amount"))
  				.from("transfers_4")
  				.where(field("toId").eq(tf.getToId()))
  				.getSQL(ParamType.INLINED);

    	Future<ResultSetGroup> creditResult = pinotConnection.executeAsync(creditQuery);
    	Future<ResultSetGroup> debitResult = pinotConnection.executeAsync(debitQuery);

    	CompletableFuture.supplyAsync(new Supplier<ResultSetGroup>() {

          @Override
          public ResultSetGroup get() {
              try {
                  return creditResult.get();
              } catch (InterruptedException | ExecutionException e) {
                  return null;
              }
          }
      }).thenAcceptBoth( 
        	CompletableFuture.supplyAsync(new Supplier<ResultSetGroup>() {

            @Override
            public ResultSetGroup get() {
                try {
                    return debitResult.get();
                } catch (InterruptedException | ExecutionException e) {
                    return null;
                }
            }
        }), (ResultSetGroup creditQueryResult, ResultSetGroup debitQueryResult) -> {
        double credit =	creditQueryResult.getResultSet(0).getDouble(0);
        double debit =	debitQueryResult.getResultSet(0).getDouble(0);

        resultFuture.complete(
        		Collections.singleton(
        				Tuple2.of(tf, credit + debit)));
      });
    }
}