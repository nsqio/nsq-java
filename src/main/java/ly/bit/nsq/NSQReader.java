package ly.bit.nsq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.AbstractLookupd;
import ly.bit.nsq.lookupd.BasicLookupdJob;
import ly.bit.nsq.util.ConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class NSQReader {
	private static final Logger log = LoggerFactory.getLogger(NSQReader.class);
	
	protected int requeueDelay;
	protected int maxRetries;
	protected int maxInFlight;
	
	protected String topic;
	protected String channel;
	protected String shortHostname;
	protected String hostname;
	
	protected ExecutorService executor;
	
	protected Class<? extends Connection> connClass;
	
	protected ConcurrentHashMap<String, Connection> connections;
	protected ConcurrentHashMap<String, AbstractLookupd> lookupdConnections;

    private ScheduledExecutorService lookupdScheduler;

	public static final ConcurrentHashMap<String, NSQReader> readerIndex = new ConcurrentHashMap<String, NSQReader>();
	
	public void init(String topic, String channel){
		this.requeueDelay = 50;
		this.maxRetries = 2;
		this.maxInFlight = 1;
		this.executor = Executors.newSingleThreadExecutor(); // TODO can be passed by caller
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
		
		this.connClass = BasicConnection.class; // TODO can be passed by caller
		this.lookupdConnections = new ConcurrentHashMap<String, AbstractLookupd>();
        this.lookupdScheduler = Executors.newScheduledThreadPool(1);

		// register action for shutdown
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run(){
				shutdown();
			}
		});
		readerIndex.put(this.toString(), this);
	}
	
	public void shutdown(){
		log.info("NSQReader received shutdown signal, shutting down connections");
		for(Connection conn: this.connections.values()){
			conn.close();
		}
        this.executor.shutdown();
        this.lookupdScheduler.shutdown();
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
				log.error("Error requeueing message to {}, will close the connection", msg.getConn());
				msg.getConn().close();
			}
		}
	}
	
	public void finishMessage(Message msg){
		try {
			msg.getConn().send(ConnectionUtils.finish(msg.getId()));
		} catch (NSQException e) {
			log.error("Error finishing message {} (from {}). Will close connection.", msg, msg.getConn());
			msg.getConn().close();
		}
	}
	
	public void connectToNsqd(String address, int port) throws NSQException{
		Connection conn;
		try {
			conn = this.connClass.newInstance();
		} catch (InstantiationException e) {
			throw new NSQException("Connection implementation must have a default constructor");
		} catch (IllegalAccessException e) {
			throw new NSQException("Connection implementation's default constructor must be visible");
		}
		conn.init(address, port, this);
		String connId = conn.toString();
		Connection stored = this.connections.putIfAbsent(connId, conn);
		if(stored != null){
			return;
		}
		conn.connect();
		for(Connection cxn : this.connections.values()){
			cxn.maxInFlight = (int) Math.ceil(this.maxInFlight / (float)this.connections.size());
		}
		conn.send(ConnectionUtils.subscribe(this.topic, this.channel, this.shortHostname, this.hostname));
		conn.send(ConnectionUtils.ready(conn.maxInFlight));
		conn.readForever();
	}
	
	
	// lookupd stuff
	
	public void addLookupd(AbstractLookupd lookupd) {
		String addr = lookupd.getAddr();
		AbstractLookupd stored = this.lookupdConnections.putIfAbsent(addr, lookupd);
		if (stored != null){
			return;
		}
        lookupdScheduler.scheduleAtFixedRate(new BasicLookupdJob(addr, this), 30, 30, SECONDS);
	}

	public String toString(){
		return "Reader<" + this.topic + ", " + this.channel + ">";
	}

	public String getTopic() {
		return topic;
	}

	public ConcurrentHashMap<String, AbstractLookupd> getLookupdConnections() {
		return lookupdConnections;
	}

}
