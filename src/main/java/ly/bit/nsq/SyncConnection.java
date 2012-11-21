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

public class SyncConnection extends Connection {
	
	private Socket sock;
	private InputStream inputStream;
	
	public SyncConnection(String host, int port, NSQReader reader){
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

}
