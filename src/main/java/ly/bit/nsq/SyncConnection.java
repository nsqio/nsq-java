package ly.bit.nsq;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

import ly.bit.nsq.exceptions.NSQException;

public class SyncConnection extends Connection {
	
	private Socket sock;
	
	public SyncConnection(String host, int port, NSQReader reader){
		this.host = host;
		this.reader = reader;
		this.port = port;
		this.sock = new Socket();
	}

	@Override
	public void send(String command) throws NSQException {
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
		this.sock.getInputStream().read(data);
		return data;
	}
	
	public byte[] readResponse() throws NSQException{
		try{
			DataInputStream dataInput = new DataInputStream(this.sock.getInputStream()); // TODO: insert buffer in there somewhere?
			int size = dataInput.readInt();
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
		}catch(IOException e){
			throw new NSQException(e);
		}
	}
	
	public void handleResponse(byte[] response) throws NSQException {
		DataInputStream ds = new DataInputStream(new ByteArrayInputStream(response));
		try {
			FrameType ft = FrameType.fromInt(ds.readInt());
			switch (ft) {
			case FRAMETYPERESPONSE:
				// do nothing?
				break;
			case FRAMETYPEMESSAGE:
				byte[] messageBytes = Arrays.copyOfRange(response, 4, response.length); 
				Message msg = this.decodeMesage(messageBytes);
				this.messageReceivedCallback(msg);
			default:
				// handle the error...
				throw new NSQException("FrameTypeError!");
			}
		} catch (IOException e) {
			throw new NSQException(e);
		}
	}

}
