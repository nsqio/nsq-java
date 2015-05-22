package ly.bit.nsq.exceptions;

/**
 * @author dan
 * 
 * RequeueWithoutBackoff is an exception that can be thrown to
 * signal that the message should be retried immediately
 *
 */
public class RequeueWithoutBackoff extends NSQException {
	private static final long serialVersionUID = 7482757109732670199L;

	public RequeueWithoutBackoff(Throwable t) {
		super(t);
	}

}
