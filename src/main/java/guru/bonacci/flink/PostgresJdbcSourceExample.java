package guru.bonacci.flink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import com.starrocks.connector.flink.StarRocksSource;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;

public class PostgresJdbcSourceExample {
	
  public static void main(String[] args) throws Exception {
//    StarRocksSourceOptions options = StarRocksSourceOptions.builder()
//           .withProperty("scan-url", "http://localhost:5432")
//           .withProperty("jdbc-url", "jdbc:postgresql://localhost:5432/postgres")
//           .withProperty("username", "baeldung")
//           .withProperty("password", "baeldung")
//           .withProperty("table-name", "tranfers")
//           .withProperty("database-name", "postgres")
//           .build();
//    
//    JdbcConnectionOptionsBuilder
//    
//    TableSchema tableSchema = TableSchema.builder()
//           .field("id", DataTypes.STRING())
//           .field("_from", DataTypes.STRING())
//           .field("_to", DataTypes.STRING())
//           .field("pool_id", DataTypes.STRING())
//           .field("amount", DataTypes.DOUBLE())
//           .field("timestamp", DataTypes.BIGINT())
//           .build();
//
//
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.addSource(StarRocksSource.source(tableSchema, options)).setParallelism(5).print();
//    env.execute("StarRocks flink source");
  }
}