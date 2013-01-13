package ly.bit.nsq.example;

import ly.bit.nsq.Message;
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
		SyncReader reader = new SyncReader("decodes", "java", sh);
//		try {
//			reader.connectToNsqd("bitly.org", 4150);
//		} catch (NSQException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		reader.addLookupd("http://bitly.org:4161");
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
