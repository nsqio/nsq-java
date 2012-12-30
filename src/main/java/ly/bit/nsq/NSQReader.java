package ly.bit.nsq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.AbstractLookupd;


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
	
	// lookupd stuff
	
	public Scheduler getLookupdScheduler() {
		// lazily create and return the scheduler
		return null;
	}
	
	public abstract AbstractLookupd makeLookupd(String addr);
	
	public synchronized void addLookupd(String addr) {
		if (this.lookupdConnections.keySet().contains(addr))
			return;
		AbstractLookupd lookupd = this.makeLookupd(addr);
		this.lookupdConnections.put(addr, lookupd);
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("lookupd-" + addr, "lookupd-triggers")
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(40)
                        .repeatForever())            
                .build();
        JobDetail lookupdJob = JobBuilder.newJob(SyncLookupdJob.class)
                .withIdentity("lookupd-" + addr, "lookupd-jobs")
        		.usingJobData("lookupdAddess", addr)
        		.build();
        try {
			this.getLookupdScheduler().scheduleJob(lookupdJob, trigger);
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private class SyncLookupdJob implements Job {

		public void execute(JobExecutionContext context)
				throws JobExecutionException {
			String addr = context.getMergedJobDataMap().getString("lookupdAddress");
			AbstractLookupd lookupd = lookupdConnections.get(addr);
			List<String> producers = lookupd.query(topic);
			for(String producer : producers) {
				String[] components = producer.split(":");
				String nsqdAddress = components[0];
				int nsqdPort = Integer.parseInt(components[1]);
				try {
					connectToNsqd(nsqdAddress, nsqdPort);
				} catch (NSQException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

}
