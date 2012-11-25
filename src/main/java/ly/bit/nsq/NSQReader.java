package ly.bit.nsq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import ly.bit.nsq.exceptions.NSQException;


public abstract class NSQReader {
	
	protected int requeueDelay;
	protected int maxRetries;
	protected int maxInFlight;
	
	protected String topic;
	protected String channel;
	protected String shortHostname;
	protected String hostname;
	
	protected ExecutorService executor;
	
	protected ConcurrentHashMap<String, Connection> connections;
	
	public void init(String topic, String channel){
		this.requeueDelay = 50;
		this.maxRetries = 2;
		this.maxInFlight = 1;
		this.executor = Executors.newSingleThreadExecutor();
		this.connections = new ConcurrentHashMap<String, Connection>();
		this.topic = topic;
		this.channel = channel;
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			this.hostname = "unknown.host";
		}
		String[] hostParts = this.hostname.split("\\.");
		this.shortHostname = hostParts[0];
	}
	
	protected abstract Runnable makeRunnableFromMessage(Message msg);
		
	public void addMessageForProcessing(Message msg){
		this.executor.execute(this.makeRunnableFromMessage(msg));
	}
	
	public void requeueMessage(Message msg, boolean doDelay){
		if(msg.getAttempts() > this.maxRetries){
			// TODO log giving up
			this.finishMessage(msg);
			return;
		}else{
			int newDelay = doDelay ? 0 : this.requeueDelay * msg.getAttempts();
			try {
				msg.getConn().send(ConnectionUtils.requeue(msg.getId(), newDelay));
			} catch (NSQException e) {
				e.printStackTrace();
				// TODO kill the connection
			}
		}
	}
	
	public void finishMessage(Message msg){
		try {
			msg.getConn().send(ConnectionUtils.finish(msg.getId()));
		} catch (NSQException e) {
			e.printStackTrace();
			// TODO kill the connection
		}
	}
	
	public void connectToNsqd(String address, int port) throws NSQException{
		Connection conn = new SyncConnection(address, port, this);
		String connId = conn.toString();
		if(this.connections.keySet().contains(connId)){
			return;
		}
		// TODO: Would like to be able to configure this a little better
		conn.connect();
		this.connections.put(connId, conn);
		for(Connection cxn : this.connections.values()){
			cxn.maxInFlight = (int) Math.ceil(this.maxInFlight / (float)this.connections.size());
		}
		conn.send(ConnectionUtils.subscribe(this.topic, this.channel, this.shortHostname, this.hostname));
		conn.send(ConnectionUtils.ready(conn.maxInFlight));
		conn.readForever();
	}

}
