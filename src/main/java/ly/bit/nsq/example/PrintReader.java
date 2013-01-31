package ly.bit.nsq.example;

import ly.bit.nsq.Message;
import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.BasicLookupd;
import ly.bit.nsq.syncresponse.SyncResponseHandler;
import ly.bit.nsq.syncresponse.SyncResponseReader;

public class PrintReader implements SyncResponseHandler {

	public boolean handleMessage(Message msg) throws NSQException {
		System.out.println(new String(msg.getBody()));
		return true;
	}

	public static void main(String... args){
		SyncResponseHandler sh = new PrintReader();
		SyncResponseReader reader = new SyncResponseReader("decodes", "java", sh);
//		try {
//			reader.connectToNsqd("bitly.org", 4150);
//		} catch (NSQException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		reader.addLookupd(new BasicLookupd("http://bitly.org:4161"));
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
