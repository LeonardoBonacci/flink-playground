package guru.bonacci.flink.domain;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transfer {
	
  private UUID id;
  private String fromId;
  private String toId;
  private String poolId;
  private String poolType;
  private double amount;
  private long timestamp;
}