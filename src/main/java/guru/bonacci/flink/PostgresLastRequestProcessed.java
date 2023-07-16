package guru.bonacci.flink;

import static org.jooq.impl.DSL.field;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import com.github.jasync.sql.db.Connection;
import com.github.jasync.sql.db.QueryResult;
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder;

import guru.bonacci.flink.domain.TransferStringWrapper;
import guru.bonacci.flink.domain.TransferValidityWrapper;

class PostgresLastRequestProcessed extends RichAsyncFunction<TransferStringWrapper, TransferValidityWrapper> {

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
    public void asyncInvoke(TransferStringWrapper tfWrapper, final ResultFuture<TransferValidityWrapper> resultFuture) throws Exception {
    	if (tfWrapper.getStr() == null) {
  	    resultFuture.complete(
  	    		Collections.singleton(
  	    				new TransferValidityWrapper(tfWrapper.getTransfer(), true)));
    		return;
    	}
    	
    	String query = DSL.using(SQLDialect.POSTGRES)
      		.select()
      		.from("transfers")
      		.where(field("id").eq(tfWrapper.getStr()))
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
        				new TransferValidityWrapper(tfWrapper.getTransfer(), queryResult.getRows().size() > 0)));
      });
    }
}