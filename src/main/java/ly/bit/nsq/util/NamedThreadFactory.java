package ly.bit.nsq.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Names threads so that thread dumps are easier to understand.
 *
 * @author oberwetter
 */
public class NamedThreadFactory implements ThreadFactory {
	final private String nameFormat;
	final private ThreadFactory decoratedFactory;
	final private AtomicLong count;

	public NamedThreadFactory(String nameFormat) {
		this(nameFormat, Executors.defaultThreadFactory());
	}

	public NamedThreadFactory(String nameFormat, ThreadFactory decoratedFactory) {
		if (nameFormat == null) {
			throw new IllegalArgumentException("nameFormat cannot be null");
		}
		// make sure the format is valid
		String.format(nameFormat, 0);

		this.nameFormat = nameFormat;
		this.decoratedFactory = decoratedFactory;
		this.count = new AtomicLong(0);
	}

	@Override
	public Thread newThread(Runnable runnable) {
		Thread thread = this.decoratedFactory.newThread(runnable);
		thread.setName(String.format(nameFormat, count.getAndIncrement()));
		return thread;
	}

}
