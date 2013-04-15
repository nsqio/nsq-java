package ly.bit.nsq.example;

/**
 * To run the examples,
 * 1. Start nsqd and nsqlookupd on your localhost.
 * 2. Start the 'ExampleProducer' in one process, this will create 100 messages to the 'testTopic' topic.
 * 3. Start this PrintReader, it will find the nsqd and start reading messages for the topic.
 *
 * Notes: You will not see any logging from the library since there are no concrete bindings for SLF4J.
 */

import ly.bit.nsq.Message;
import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.BasicLookupd;
import ly.bit.nsq.syncresponse.SyncResponseHandler;
import ly.bit.nsq.syncresponse.SyncResponseReader;

public class PrintReader implements SyncResponseHandler {

	public boolean handleMessage(Message msg) throws NSQException {
		System.out.println("Received: " + new String(msg.getBody()));
		return true;
	}

	public static void main(String... args){
		SyncResponseHandler sh = new PrintReader();
		SyncResponseReader reader = new SyncResponseReader("testTopic", "java#ephemeral", sh);
//		try {
//			reader.connectToNsqd("bitly.org", 4150);
//		} catch (NSQException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		reader.addLookupd(new BasicLookupd("http://127.0.0.1:4161"));
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
