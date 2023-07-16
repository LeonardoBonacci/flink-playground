package guru.bonacci.flink;

import static org.jooq.impl.DSL.field;

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

class PostgresLastRequestProcessed extends RichAsyncFunction<Tuple2<Transfer, String>, Tuple2<Transfer, Boolean>> {

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
      		.from("transfers")
      		.where(field("id").eq(tfTuple.f1))
      		.getSQL(ParamType.INLINED);

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
        				Tuple2.of(tfTuple.f0, queryResult.getRows().size() > 0)));
      });
    }
}