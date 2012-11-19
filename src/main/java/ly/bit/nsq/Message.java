package ly.bit.nsq;

public class Message {
	
	public Message(byte[] id, byte[] body, long timestamp, short attempts, Connection conn) {
		super();
		this.id = id;
		this.body = body;
		this.timestamp = timestamp;
		this.attempts = attempts;
		this.conn = conn;
	}
	
	private byte[] id;
	private byte[] body;
	private long timestamp;
	private short attempts;
	private Connection conn;

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

	public Connection getConn() {
		return conn;
	}


}
