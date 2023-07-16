package guru.bonacci.flink;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class TransferGenerator extends RichSourceFunction<Transfer> {

	private final static List<String> USERS = 
			List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
	private boolean running = true;

	@Override
	public void run(SourceContext<Transfer> sourceContext) throws Exception {
		while (this.running) {
			final ThreadLocalRandom random = ThreadLocalRandom.current();
			var tf = Transfer.builder()
									.id(UUID.randomUUID())
									.poolId("coro")
									.from(USERS.get(random.nextInt(USERS.size())))
									.to(USERS.get(random.nextInt(USERS.size())))
									.amount(random.nextDouble(0, 100))
									.timestamp(System.currentTimeMillis())
									.build();

			sourceContext.collect(tf);
			Thread.sleep(10);
		}
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}