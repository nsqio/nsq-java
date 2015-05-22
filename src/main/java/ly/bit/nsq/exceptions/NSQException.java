package ly.bit.nsq.exceptions;

public class NSQException extends Exception {
	private static final long serialVersionUID = 7412096973790239373L;

	public NSQException(Throwable t) {
		super(t);
	}

	public NSQException(String string) {
		super(string);
	}

}
