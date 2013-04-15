package ly.bit.nsq;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.util.ConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicConnection extends Connection {
	private static final Logger log = LoggerFactory.getLogger(BasicConnection.class);
	
	private Socket sock;
	private InputStream inputStream;
	
	public void init(String host, int port, NSQReader reader){
		this.host = host;
		this.reader = reader;
		this.port = port;
		this.sock = new Socket();
	}

	@Override
	public synchronized void send(String command) throws NSQException {
		try {
			OutputStream os = this.sock.getOutputStream();
			os.write(command.getBytes());
		} catch (IOException e) {
			throw new NSQException(e);
		}
	}
	
	public byte[] readN(int size) throws IOException{
		// Going with a super naive impl first...
		byte[] data = new byte[size];
		this.inputStream.read(data);
		return data;
	}
	
	public byte[] readResponse() throws NSQException{
		try{
			DataInputStream ds = new DataInputStream(this.inputStream);
			int size = ds.readInt();
			byte[] data = this.readN(size);
			return data;
		}catch(IOException e){
			throw new NSQException(e);
		}
	}

	@Override
	public void connect() throws NSQException {
		try{
			this.sock.connect(new InetSocketAddress(host, port));
			this.send(ConnectionUtils.MAGIC_V2);
			this.inputStream = new BufferedInputStream(this.sock.getInputStream());
		}catch(IOException e){
			throw new NSQException(e);
		}
	}

	@Override
	public void readForever() {
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
		try {
			this.sock.close();
		} catch (IOException e) {
			// whatever, we're not doing anything with this anymore
			log.error("Exception closing connection: ", e);
		}
		this.reader.connections.remove(this.toString());
	}

}
