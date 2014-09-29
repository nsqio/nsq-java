package ly.bit.nsq.syncresponse;

import ly.bit.nsq.Message;
import ly.bit.nsq.MockConnection;
import ly.bit.nsq.exceptions.NSQException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * User: oneill
 * Date: 4/8/13
 */
public class SyncResponseReaderTest {
	Logger log = LoggerFactory.getLogger(SyncResponseReaderTest.class);

	@Test
	public void testIncomingMessage_success() throws NSQException, InterruptedException {
		// Setup a dummy handler which records the incoming messages
		MockHandler handler = new MockHandler();
		SyncResponseReader reader = new SyncResponseReader("testTopic", "testChannel", handler);
		reader.setConnClass(MockConnection.class);
		reader.connectToNsqd("127.0.0.1", 4151);

		// Get the mock connection and inject a message into it
		MockConnection conn = (MockConnection) reader.getConnections().get("127.0.0.1:4151");
		assertNotNull(conn);

		String body = "{\"foo\":\"bar\"}";
		byte[] idBytes = MockConnection.randomId();
		String idString = new String(idBytes);
		conn.fakeMessage(idBytes, body);

		Thread.sleep(3000);

		// Compare the injected and received messages
		List<Message> received = handler.getReceived();
		assertEquals(1, received.size()); // received one message

		// handler was successful, we ack the message with "FIN $id"
		String lastSent = conn.getLastSent();
		assertEquals("FIN " + idString + "\n", lastSent);
	}

	/**
	 * Test the case where the handler rejects the message (e.g. database down).
	 * @throws NSQException
	 * @throws InterruptedException
	 */
	@Test
	public void testIncomingMessage_failure() throws NSQException, InterruptedException {
		// Setup a dummy handler which records the incoming messages
		MockHandler handler = new MockHandler();
		SyncResponseReader reader = new SyncResponseReader("testTopic", "testChannel", handler);
		reader.setConnClass(MockConnection.class);
		reader.connectToNsqd("127.0.0.1", 4151);
		handler.failNext();

		// Get the mock connection and inject a message into it
		MockConnection conn = (MockConnection) reader.getConnections().get("127.0.0.1:4151");
		assertNotNull(conn);

		String body = "{\"foo\":\"bar\"}";
		byte[] idBytes = MockConnection.randomId();
		String idString = new String(idBytes);
		conn.fakeMessage(idBytes, body);

		Thread.sleep(3000);

		// Compare the injected and received messages
		List<Message> received = handler.getReceived();
		assertEquals(1, received.size());

		// handler had an error, we will try to requeue the message for the first time
		String lastSent = conn.getLastSent();
		assertEquals("REQ " + idString + " 0\n", lastSent);

	}
}
