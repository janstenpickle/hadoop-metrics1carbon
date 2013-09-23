package org.apache.hadoop.metrics.tools;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 23/09/2013
 * Time: 11:12
 * To change this template use File | Settings | File Templates.
 */

import java.util.Random;


public class ExponentialBackoff  {
	private final int MAX_SHIFT = 30;

	private final Random random = new Random();
	private final int baseSleepTimeMs;
	private int attempts;

	public ExponentialBackoff(int baseSleepTimeMs) {
		this.baseSleepTimeMs = baseSleepTimeMs;
		this.attempts = 0;
	}

	public long getSleepTimeMs() {
		int attempt = this.getAttemptCount() + 1;
		if (attempt > MAX_SHIFT) {
			attempt = MAX_SHIFT;
		}
		attempts ++;
		return baseSleepTimeMs * Math.max(1, random.nextInt(1 << attempt));
	}

	public int getAttemptCount() {
		return attempts;
	}

	public int getBaseSleepTimeMs() {
		return baseSleepTimeMs;
	}

}
