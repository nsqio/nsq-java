package ly.bit.nsq;

public class Message {
	
	public Message(byte[] id, byte[] body, long timestamp, short attempts) {
		super();
		this.id = id;
		this.body = body;
		this.timestamp = timestamp;
		this.attempts = attempts;
	}
	
	private byte[] id;
	private byte[] body;
	private long timestamp;
	private short attempts;
	
	public static Message Decode(byte[] data) {
		// TODO: load fields from message data. see https://github.com/bitly/pynsq/blob/master/nsq/nsq.py#L24
		return null;
	}

	public byte[] getId() {
		return id;
	}

	public void setId(byte[] id) {
		this.id = id;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public short getAttempts() {
		return attempts;
	}

	public void setAttempts(short attempts) {
		this.attempts = attempts;
	}

}
