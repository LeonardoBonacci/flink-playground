package guru.bonacci.flink;

import static org.jooq.impl.DSL.field;

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

class PinotLastRequestProcessed extends RichAsyncFunction<Tuple2<Transfer, String>, Tuple2<Transfer, Boolean>> {

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

    @Override
    public void asyncInvoke(Tuple2<Transfer, String> tfTuple, final ResultFuture<Tuple2<Transfer, Boolean>> resultFuture) throws Exception {
    	if (tfTuple.f1 == null) {
  	    resultFuture.complete(
  	    		Collections.singleton(
  	    				Tuple2.of(tfTuple.f0, true)));
    		return;
    	}
    	
    	String query = DSL.using(SQLDialect.POSTGRES)
      		.select()
      		.from("transfers_4")
      		.where(field("id").eq(tfTuple.f1))
      		.getSQL(ParamType.INLINED);

    	Future<ResultSetGroup> result = pinotConnection.executeAsync(query);
    	CompletableFuture.supplyAsync(new Supplier<ResultSetGroup>() {

          @Override
          public ResultSetGroup get() {
              try {
                  return result.get();
              } catch (InterruptedException | ExecutionException e) {
                  return null;
              }
          }
      }).thenAccept( (ResultSetGroup resultSetGroup) -> {
        resultFuture.complete(
        		Collections.singleton(
        				Tuple2.of(tfTuple.f0, resultSetGroup.getResultSet(0).getRowCount() > 0)));
      });
    }
}