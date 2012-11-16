package ly.bit.nsq;

import java.util.concurrent.ThreadPoolExecutor;


public abstract class NSQReader {
	
	protected ThreadPoolExecutor executor;
	
	protected abstract Runnable makeRunnableFromMessage(Message msg, Connection conn);
		
	public void addMessageForProcessing(Message msg, Connection conn){
		this.executor.execute(this.makeRunnableFromMessage(msg, conn));
	}

}
