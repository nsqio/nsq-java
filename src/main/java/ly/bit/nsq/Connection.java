package ly.bit.nsq;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import ly.bit.nsq.exceptions.NSQException;


/**
 * @author dan
 *
 * This class (which we may want to make abstract later or something) should manage
 * the connection to an instance of nsqd. It should have methods to send commands to nsqd,
 * which I guess will get into the Netty stuff that you guys were talking about, as well as
 * I guess some sort of callback function on data received - is that how Netty works?
 * 
 * Anyway, I'm going to stub out what I think it should do when it receives a new message.
 * We can all revisit as we flesh more stuff out.
 *
 */
public abstract class Connection {
	
	protected NSQReader reader;
	protected String host;
	protected int port;
	
	public void messageReceivedCallback(Message message){
		this.reader.addMessageForProcessing(message);
	}
	
	public abstract void send(String command) throws NSQException;
	public abstract void connect() throws NSQException;
	
	public Message decodeMesage(byte[] data) throws NSQException {
		DataInputStream ds = new DataInputStream(new ByteArrayInputStream(data));
		try {
			long timestamp = ds.readLong(); // 8 bytes
			short attempts = ds.readShort(); // 2 bytes
			byte[] id = new byte[16];
			ds.read(id);
			byte[] body = new byte[data.length - 26];
			ds.read(body);
			return new Message(id, body, timestamp, attempts, this);
		} catch (IOException e) {
			throw new NSQException(e);
		}
	}

}
