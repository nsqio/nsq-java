package ly.bit.nsq.syncresponse;

import ly.bit.nsq.Message;
import ly.bit.nsq.exceptions.NSQException;

/**
 * @author dan
 *
 * A SyncHandler processes a message and indicates success synchronously.
 * It returns true to indicate successful processing, and returns false to
 * indicate that there was an error and the message should be requeued.
 * 
 * Messages will also be requeued if an exception is thrown. RequeueWithoutBackoff
 * can be thrown to trigger an immediate requeue.
 *
 */
public interface SyncResponseHandler {
	
	public boolean handleMessage(Message msg) throws NSQException;

}
