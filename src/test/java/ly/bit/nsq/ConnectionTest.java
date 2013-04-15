package ly.bit.nsq;

import ly.bit.nsq.exceptions.NSQException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * User: oneill
 * Date: 4/8/13
 */
public class ConnectionTest {
	Logger log = LoggerFactory.getLogger(ConnectionTest.class);


	/**
	 * Test that we can encode and decode a message properly.
	 * @throws NSQException
	 */
	@Test
	public void testCodec() throws NSQException {

		byte[] id = MockConnection.randomId();


		Connection conn = new MockConnection();
		String body = "kjehfliuANDY.WAS.HEREe;flijwe,jfhwqliuehfj";
		Message msg = new Message(id, body.getBytes(), System.currentTimeMillis()*1000,
				new Integer(0).shortValue(), conn);
		byte[] encoded = conn.encodeMessage(msg);
		log.debug("Encoded message: {}", encoded);

		Message decoded = conn.decodeMessage(encoded);
		assertEquals(msg.getAttempts(), decoded.getAttempts());
		for (int i=0; i<id.length; i++) {
			assertEquals(id[i], decoded.getId()[i]);
		}
		assertEquals(new String(msg.getBody()), new String(decoded.getBody()));
		assertEquals(msg.getTimestamp(), decoded.getTimestamp());

		byte[] reenecoded = conn.encodeMessage(decoded);
		assertEquals(encoded.length, reenecoded.length);
		for (int i=0; i<reenecoded.length; i++) {
			assertEquals(encoded[i], reenecoded[i]);
		}

	}
}
