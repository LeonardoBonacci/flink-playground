package guru.bonacci.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferValidityWrapper {
	
  private Transfer transfer;
  private boolean valid;
}