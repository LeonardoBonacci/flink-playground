package guru.bonacci.flink;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transfer {
	
  private UUID id;
  private String from;
  private String to;
  private String poolId;
  private double amount;
  private long timestamp;
  
  public String getIdentifier() {
  	return poolId + "_" + from;
  }
}