package ly.bit.nsq;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: oneill
 * Date: 4/5/13
 */
public class NSQReaderTest {
	Logger log = LoggerFactory.getLogger(NSQReaderTest.class);


	@Test
	public void testSetConnections() throws Exception {

		Set<String> firstResponse = new HashSet<String>();
		firstResponse.add("producerA:4151");
		firstResponse.add("producerB:4151");
		firstResponse.add("producerC:4151");

		Set<String> secondResponse = new HashSet<String>();
		secondResponse.add("producerB:4151");
		secondResponse.add("producerC:4151");
		secondResponse.add("producerD:4151");

		NSQReader reader = new NSQReader() {
			@Override
			protected Runnable makeRunnableFromMessage(Message msg) {
				return new Runnable(){
					public void run() {

					}
				};

			}
		};

		reader.connClass = MockConnection.class;
		reader.init("testTopic", "testChannel");

		log.debug("Setting first lookupd response (A,B,C)");
		reader.handleLookupdResponse(firstResponse);

		assertEquals(firstResponse.size(), reader.connections.size());
		assertTrue(reader.connections.containsKey("producerA:4151"));
		assertTrue(reader.connections.containsKey("producerB:4151"));
		assertTrue(reader.connections.containsKey("producerC:4151"));


		log.debug("Setting second lookupd response (B,C,D)");
		reader.handleLookupdResponse(secondResponse);
		assertEquals(secondResponse.size(), reader.connections.size());
		assertTrue(reader.connections.containsKey("producerB:4151"));
		assertTrue(reader.connections.containsKey("producerC:4151"));
		assertTrue(reader.connections.containsKey("producerD:4151"));


	}

}
