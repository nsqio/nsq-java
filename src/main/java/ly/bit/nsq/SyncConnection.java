package ly.bit.nsq;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import ly.bit.nsq.exceptions.NSQException;

public class SyncConnection extends Connection {
	
	private Socket sock;

	@Override
	public void send(String command) throws NSQException {
		try {
			OutputStream os = this.sock.getOutputStream();
			os.write(command.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new NSQException();
		}

	}
	
	public byte[] readN(int size){
		// Going with a super naive impl first...
		byte[] data = new byte[size];
		try {
			this.sock.getInputStream().read(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return data;
	}
	
	public byte[] readResponse() throws NSQException{
		try{
			DataInputStream dataInput = new DataInputStream(this.sock.getInputStream()); // TODO: insert buffer in there somewhere?
			int size = dataInput.readInt();
			byte[] data = this.readN(size);
			return data;
		}catch(IOException e){
			throw new NSQException();
		}
	}

}
