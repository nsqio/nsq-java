package ly.bit.nsq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.AbstractLookupd;
import ly.bit.nsq.lookupd.SyncLookupdJob;


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
	protected ConcurrentHashMap<String, AbstractLookupd> lookupdConnections;
	
	private Scheduler scheduler;
	
	public static final ConcurrentHashMap<String, NSQReader> readerIndex = new ConcurrentHashMap<String, NSQReader>();
	
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
		
		this.lookupdConnections = new ConcurrentHashMap<String, AbstractLookupd>();
		try {
			this.scheduler = StdSchedulerFactory.getDefaultScheduler();
			this.scheduler.start();
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
		System.out.println("Received signal to shut down");
		try {
			this.scheduler.shutdown();
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(Connection conn: this.connections.values()){
			conn.close();
		}
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
				msg.getConn().close();
			}
		}
	}
	
	public void finishMessage(Message msg){
		try {
			msg.getConn().send(ConnectionUtils.finish(msg.getId()));
		} catch (NSQException e) {
			e.printStackTrace();
			msg.getConn().close();
		}
	}
	
	public void connectToNsqd(String address, int port) throws NSQException{
		Connection conn = new SyncConnection(address, port, this);
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
	
	public Scheduler getLookupdScheduler() {
		return this.scheduler;
	}
	
	public abstract AbstractLookupd makeLookupd(String addr);
	
	public void addLookupd(String addr) {
		AbstractLookupd lookupd = this.makeLookupd(addr);
		AbstractLookupd stored = this.lookupdConnections.putIfAbsent(addr, lookupd);
		if(stored != null){
			return;
		}
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("lookupd-" + addr, "lookupd-triggers")
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(40)
                        .repeatForever())            
                .build();
        JobDetail lookupdJob = JobBuilder.newJob(SyncLookupdJob.class)
                .withIdentity("lookupd-" + addr, "lookupd-jobs")
        		.usingJobData("lookupdAddress", addr)
        		.usingJobData("reader", this.toString())
        		.build();
        try {
			this.getLookupdScheduler().scheduleJob(lookupdJob, trigger);
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// -----
	
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
