package ly.bit.nsq;


import ly.bit.nsq.exceptions.NSQException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MessageCodec {
	public static Message decode(byte[] data, Connection conn) throws NSQException {
		DataInputStream ds = new DataInputStream(new ByteArrayInputStream(data));
		try {
			long timestamp = ds.readLong(); // 8 bytes
			short attempts = ds.readShort(); // 2 bytes
			byte[] id = new byte[16];
			ds.read(id);
			byte[] body = new byte[data.length - 26];
			ds.read(body);
			return new Message(id, body, timestamp, attempts, conn);
		} catch (IOException e) {
			throw new NSQException(e);
		}
	}

	/**
	 * Reverse of decodeMessage, helpful in testing so far.
	 * @param msg
	 * @return
	 * @throws NSQException
	 */
	public static byte[] encode(Message msg) throws NSQException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream ds = new DataOutputStream(bytes);
		try {
			ds.writeLong(msg.getTimestamp());
			ds.writeShort(msg.getAttempts());
			ds.write(msg.getId());
			ds.write(msg.getBody());
			ds.close();
		} catch (IOException e) {
			throw new NSQException(e);
		}
		return bytes.toByteArray();
	}

}
