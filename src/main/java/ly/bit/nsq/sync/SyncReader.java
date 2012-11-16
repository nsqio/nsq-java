package ly.bit.nsq.sync;

import ly.bit.nsq.Connection;
import ly.bit.nsq.Message;
import ly.bit.nsq.NSQReader;

public class SyncReader extends NSQReader {
	
	private SyncHandler handler;
	
	private class SyncMessageRunnable implements Runnable {
		
		public SyncMessageRunnable(Message msg, Connection conn) {
			super();
			this.msg = msg;
			this.conn = conn;
		}

		private Message msg;
		private Connection conn;

		public void run() {
			boolean success = false;
			try{
				success = handler.handleMessage(msg);
			}catch(Exception e){
				// TODO
			}
			// tell conn about success or failure
			
		}
	}

	@Override
	protected Runnable makeRunnableFromMessage(Message msg, Connection conn) {
		return new SyncMessageRunnable(msg, conn);
	}

}
