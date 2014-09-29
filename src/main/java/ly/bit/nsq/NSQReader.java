package ly.bit.nsq;

/**
 * Base implementation of an NSQ consusumer client which manages lookupd requests
 * and responses, nsqd connections etc.
 * Typically you would use a specific implementation like SyncResponseReader, which
 * additionally implements synchronous responses for messages.
 * See the PrintReader example.
 */

import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.AbstractLookupd;
import ly.bit.nsq.lookupd.BasicLookupdJob;
import ly.bit.nsq.util.ConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;


public abstract class NSQReader {
	private static final Logger log = LoggerFactory.getLogger(NSQReader.class);

	private static final int LOOKUPD_INITIAL_DELAY = 0;
	private static final int LOOKUPD_POLL_INTERVAL = 30;
	
	protected int requeueDelay;
	protected int maxRetries;
	protected int maxInFlight;
	
	protected String topic;
	protected String channel;
	protected String shortHostname;
	protected String hostname;
	
	protected ExecutorService executor;
	
	protected Class<? extends Connection> connClass = BasicConnection.class;
	
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

	/**
	 * Set the currently configured nsqd addresses that we should be connected to. This triggers disconnects (from
	 * disused servers) and connection attempts (to new servers).
	 * This function is probably only called from the Lookupd job.
	 * @param connectionAddresses
	 */
	public void handleLookupdResponse(Set<String> connectionAddresses) {
		Set<String> toClose = new HashSet<String>(connections.keySet());
		Set<String> toOpen = new HashSet<String>(connectionAddresses);

		// We need to open any connections in set that are not already open
		toOpen.removeAll(connections.keySet());
		// We need to close any connections that are open but not in the set
		toClose.removeAll(connectionAddresses);

		// Open new connections
		for(String address : toOpen) {
			log.info("Opening new producer connection: {}", address);
			String[] components = address.split(":");
			String nsqdAddress = components[0];
			int nsqdPort = Integer.parseInt(components[1]);
			try {
				connectToNsqd(nsqdAddress, nsqdPort);
			} catch (NSQException e) {
				log.error("Erroring response from lookupd", e);
			}
		}
		// close old connections
		for(String address : toClose) {
			if (connections.containsKey(address)) {
				log.info("Producer not in lookupd response, closing connection: {}", address);
				connections.get(address).close();
				this.connections.remove(address);
			}

		}
	}

	/**
	 * Remove this nsqd connection from our active pool if it is present.
	 * Typically called after problems are detected with the connection, or
	 * after a lookupd request shows that the server is no longer active.
	 * @param conn
	 */
	public void connectionClosed(Connection conn) {
		this.connections.remove(conn.toString());

	}
	
	// lookupd stuff
	
	public void addLookupd(AbstractLookupd lookupd) {
		String addr = lookupd.getAddr();
		AbstractLookupd stored = this.lookupdConnections.putIfAbsent(addr, lookupd);
		if (stored != null){
			return;
		}
        lookupdScheduler.scheduleAtFixedRate(new BasicLookupdJob(addr, this), LOOKUPD_INITIAL_DELAY, LOOKUPD_POLL_INTERVAL, SECONDS);
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

	public void setConnClass(Class<? extends Connection> clazz) {
		this.connClass = clazz;
	}

	public ConcurrentHashMap<String, Connection> getConnections() {
		return this.connections;
	}

}
