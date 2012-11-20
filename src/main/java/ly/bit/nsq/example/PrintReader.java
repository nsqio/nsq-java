package ly.bit.nsq.example;

import ly.bit.nsq.ConnectionUtils;
import ly.bit.nsq.Message;
import ly.bit.nsq.SyncConnection;
import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.sync.SyncHandler;
import ly.bit.nsq.sync.SyncReader;

public class PrintReader implements SyncHandler {

	public boolean handleMessage(Message msg) throws NSQException {
		System.out.println(new String(msg.getBody()));
		return true;
	}

	public static void main(String... args){
		SyncHandler sh = new PrintReader();
		SyncReader reader = new SyncReader(sh);
		SyncConnection conn = new SyncConnection("bitly.org", 4150, reader);
		try{
			conn.connect();
			conn.send(ConnectionUtils.subscribe("test", "java", "df", "danielhfrank"));
			for(int i = 0; i < 10; i++){
				conn.send(ConnectionUtils.ready(1));
				byte[] response = conn.readResponse();
				conn.handleResponse(response);	
			}
			
		}catch(NSQException e){
			e.printStackTrace();
		}
	}

}
