package ly.bit.nsq;

import ly.bit.nsq.exceptions.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Mock connection implementation for unit tests that does attempt real TCP connections.
 * Not very useful for concurrent testing, but it does keep track of the last message sent,
 * and allow one to simulate receiving a message via the 'messages' queue.
 * User: oneill
 * Date: 4/5/13
 */
public class MockConnection extends Connection {
	Logger log = LoggerFactory.getLogger(MockConnection.class);
	private String lastSent = "";
	private boolean connected = false;

	private BlockingQueue<byte[]> messages = new ArrayBlockingQueue<byte[]>(10);

	@Override
	public void init(String host, int port, NSQReader reader) {
		this.host = host;
		this.port = port;
		this.reader = reader;
		this.closed.getAndSet(false);
		log.debug("Init mock connection {}:{}", host, port);

	}

	@Override
	public void send(String command) throws NSQException {
		log.debug("Sending '{}'", command);
		lastSent = command;
	}

	@Override
	public void connect() throws NSQException {
		log.debug("{} is connecting", this.toString());
	}

	private byte[] readResponse() throws NSQException {
		try {
			byte[] msg = messages.take();
			return msg;
		} catch (InterruptedException e) {
			throw new NSQException(e);
		}
	}

	@Override
	public void readForever() throws NSQException {
		class ReadThis implements Runnable {
			public void run() {
				while(closed.get() != true){
					byte[] response;
					try {
						response = readResponse();
					} catch (NSQException e) {
						// Assume this meant that we couldn't read somehow, should close the connection
						close();
						break;
					}
					try {
						handleResponse(response);
					} catch (NSQException e) {
						// malformed message or something...
						log.error("Message error: ", e);
					}
				}
			}
		}
		Thread t = new Thread(new ReadThis(), this.toString());
		t.start();
	}

	@Override
	public void close() {
		boolean prev = this.closed.getAndSet(true);
		if(prev == true){
			return;
		}
		log.info("Closing connection {}", this.toString());
		MockConnection.this.reader.connectionClosed(MockConnection.this);
	}

	public void fakeMessage(byte[] idBytes, String body) throws NSQException {

		Message msg = new Message(idBytes, body.getBytes(), System.currentTimeMillis()*1000,
				new Integer(0).shortValue(), this);

		byte[] framed = frameMessage(this.encodeMessage(msg));
		log.debug("Inserting fake message: '{}'", framed);

		this.messages.add(framed);
	}

	/**
	 * Helper for creating 16-byte ids
	 * @return
	 */
	public static byte[] randomId() {
		return UUID.randomUUID().toString().replace("-", "").substring(0, 16).getBytes();
	}

	public static byte[] frameMessage(byte[] msg) throws NSQException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try {
			DataOutputStream ds = new DataOutputStream(bytes);
			ds.writeInt(2);
			ds.write(msg);
			ds.flush();
			return bytes.toByteArray();
		} catch(IOException e) {
			throw new NSQException(e);
		}
	}

	public String getLastSent() {
		return this.lastSent;
	}

}
