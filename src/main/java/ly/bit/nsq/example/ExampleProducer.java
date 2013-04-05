package ly.bit.nsq.example;

import ly.bit.nsq.NSQProducer;
import ly.bit.nsq.exceptions.NSQException;

public class ExampleProducer {

	public static void main(String... args){
		NSQProducer producer = new NSQProducer("http://127.0.0.1:4151", "testTopic");

		for(int i=0; i<100; i++) {
			try {
				String message = "{\"foo\":\"bar\"}";
				System.out.println("Sending: " + message);
				producer.put(message);
				Thread.sleep(1000);
			} catch (NSQException e) {
				e.printStackTrace();
				System.exit(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
