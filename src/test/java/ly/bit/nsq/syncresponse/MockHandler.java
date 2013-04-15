package ly.bit.nsq.syncresponse;

import ly.bit.nsq.Message;
import ly.bit.nsq.exceptions.NSQException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A mock implementation of SyncResponseHandler that tracks incoming
 * messages for unit tests
 */
public class MockHandler implements SyncResponseHandler {
	private List<Message> received;
	private boolean failNextMessage = false;

	public MockHandler() {
		this.received = Collections.synchronizedList(new ArrayList<Message>());
	}

	@Override
	public boolean handleMessage(Message msg) throws NSQException {
		received.add(msg);

		boolean ret = true;
		synchronized(this) {
			if (failNextMessage) {
				failNextMessage = false;
				ret = false;
			}
		}
		return ret;
	}

	/**
	 * For testing, will cause the next message handling to fail.
	 */
	public void failNext() {
		synchronized (this) {
			this.failNextMessage = true;
		}
	}
	public List<Message> getReceived() {
		return this.received;
	}
}
