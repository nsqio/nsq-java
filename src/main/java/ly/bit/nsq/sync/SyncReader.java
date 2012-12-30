package ly.bit.nsq.sync;

import ly.bit.nsq.Message;
import ly.bit.nsq.NSQReader;
import ly.bit.nsq.exceptions.RequeueWithoutBackoff;
import ly.bit.nsq.lookupd.AbstractLookupd;
import ly.bit.nsq.lookupd.SyncLookupd;

public class SyncReader extends NSQReader {
	
	private SyncHandler handler;
	
	public SyncReader(String topic, String channel, SyncHandler handler) {
		super();
		this.handler = handler;
		this.init(topic, channel);
	}

	private class SyncMessageRunnable implements Runnable {
		
		public SyncMessageRunnable(Message msg) {
			super();
			this.msg = msg;
		}

		private Message msg;

		public void run() {
			boolean success = false;
			boolean doDelay = true;
			try{
				success = handler.handleMessage(msg);
			}catch(RequeueWithoutBackoff e){
				doDelay = false;
			}catch(Exception e){
				// do nothing, success already false
			}
			
			// tell conn about success or failure
			if(success){
				finishMessage(msg);
			}else{
				requeueMessage(msg, doDelay);
			}
		}
	}

	@Override
	protected Runnable makeRunnableFromMessage(Message msg) {
		return new SyncMessageRunnable(msg);
	}

	@Override
	public AbstractLookupd makeLookupd(String addr) {
		return new SyncLookupd(addr);
	}
}
