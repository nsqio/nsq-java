package ly.bit.nsq.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class NamedThreadFactoryTest {

	@Test
	public void testStuff() {
		class MyRunnable implements Runnable {
			@Override
			public void run() {
				// do nothing
			}
		}
		NamedThreadFactory factory = new NamedThreadFactory("TheFactoryWuzHere-%d");

		Thread t1 = factory.newThread(new MyRunnable());
		assertEquals("TheFactoryWuzHere-0", t1.getName());

		Thread t2 = factory.newThread(new MyRunnable());
		assertEquals("TheFactoryWuzHere-1", t2.getName());
	}

}
