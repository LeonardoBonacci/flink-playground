package guru.bonacci.flink;

import static org.jooq.impl.DSL.field;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class PinotLastRequestProcessed extends RichAsyncFunction<TransferStringWrapper, TransferValidityWrapper> {

		private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
    	connection = ConnectionFactory.fromHostList("localhost:8099");
    }

    @Override
    public void close() throws Exception {
    	connection.close();
    }

    @Override
    public void asyncInvoke(TransferStringWrapper tfWrapper, final ResultFuture<TransferValidityWrapper> resultFuture) throws Exception {
    	final String query = DSL.using(SQLDialect.POSTGRES)
    		.select().from("transfers").where(field("ID").eq(tfWrapper.getStr())).toString();
    	
//    	PreparedStatement statement =
//    	    connection.prepareStatement(new Request("sql", "select * from transfers where id = ?"));
//    	statement.setString(1, tfWrapper.getStr());

    	Future<ResultSetGroup> result = connection.executeAsync(query);
    	connection.executeAsync(query);
//    	Future<ResultSetGroup> result = statement.executeAsync();
    	
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
      	ResultSet resultTableResultSet = resultSetGroup.getResultSet(0);
      	int numRows = resultTableResultSet.getRowCount();
      	log.info("rows " + numRows);
      	boolean isProcessed = numRows > 0;
        resultFuture.complete(
        		Collections.singleton(
        				TransferValidityWrapper.builder().transfer(tfWrapper.getTransfer()).valid(isProcessed).build()));
      });
    }
}