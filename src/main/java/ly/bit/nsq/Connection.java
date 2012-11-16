package ly.bit.nsq;


/**
 * @author dan
 *
 * This class (which we may want to make abstract later or something) should manage
 * the connection to an instance of nsqd. It should have methods to send commands to nsqd,
 * which I guess will get into the Netty stuff that you guys were talking about, as well as
 * I guess some sort of callback function on data received - is that how Netty works?
 * 
 * Anyway, I'm going to stub out what I think it should do when it receives a new message.
 * We can all revisit as we flesh more stuff out.
 *
 */
public class Connection {
	
	protected NSQReader reader;
	
	public void messageReceivedCallback(Message message){
		this.reader.addMessageForProcessing(message, this);
	}

}
