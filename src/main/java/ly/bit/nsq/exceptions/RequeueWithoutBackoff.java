package ly.bit.nsq.exceptions;

/**
 * @author dan
 * 
 * RequeueWithoutBackoff is an exception that can be thrown to
 * signal that the message should be retried immediately
 *
 */
public class RequeueWithoutBackoff extends NSQException {

	public RequeueWithoutBackoff(Throwable t) {
		super(t);
	}

}
