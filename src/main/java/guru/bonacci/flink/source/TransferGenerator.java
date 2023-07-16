package guru.bonacci.flink.source;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import guru.bonacci.flink.domain.Transfer;

public class TransferGenerator extends RichSourceFunction<Transfer> {

	private final static List<String> USERS = 
			List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");

	private final static List<String> POOL_TYPES = 
			List.of(RuleGenerator.EASY, RuleGenerator.MEDIUM, RuleGenerator.HARD);

	private boolean running = true;

	@Override
	public void run(SourceContext<Transfer> sourceContext) throws Exception {
		while (this.running) {
			final ThreadLocalRandom random = ThreadLocalRandom.current();
			var tf = new Transfer(
									UUID.randomUUID(), // id
									USERS.get(random.nextInt(USERS.size())), // from
									USERS.get(random.nextInt(USERS.size())), // to
									"coro", // poolId
									POOL_TYPES.get(random.nextInt(POOL_TYPES.size())), // poolType
									random.nextDouble(0, 100), // amount
									System.currentTimeMillis() // timestamp
								);

			sourceContext.collect(tf);
			Thread.sleep(10);
		}
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}